# 拥塞丢链接修复说明

针对 `s0rg/crawley@abf09cdfa92e1e5c14b8e1dfa848c3175dd23c0b` 在「发现 / 待抓 / 输出」
队列拥塞时超时丢链接的问题，本改动在不回退为串行抓取的前提下，保证**同一个有限站点，
无论输出多慢、扇出多大，最终采集到的 URL 集合保持不变**。

## 1. 问题根因

原实现用一组**无界 channel 配合超时 select**在各阶段之间传递数据。当下游（输出回调、
HTTP 抓取）变慢，而上游持续产出大扇出页面时：

- channel 满，生产者在 `select { case ch <- x: … case <-time.After(d): }` 上超时；
- 超时分支直接放弃该链接，导致合规 URL 既不被抓取也不被输出；
- 越拥塞丢得越多，采集结果随运行速度而变化，违反「慢输出不改变采集集合」。

无界 channel 同时意味着内存随图规模线性增长，没有明确上限。

## 2. 总体方案

核心思路：**有界内存 + 透明磁盘溢写 + 精确终止计数**，用背压（back-pressure）取代
超时丢弃。任何一个缓冲区写满后，新元素落盘而不是被丢弃，因此内存恒定、集合完整。

新增 / 修改的组件：

| 组件 | 职责 |
| --- | --- |
| `internal/queue/queue.go`（新增） | 泛型有界 FIFO 队列：定长内存环形缓冲，满后溢写到临时 JSON 文件 |
| `internal/crawler/crawler.go`（重写运行时） | 固定 worker 池 + 单一协调器 + 单一输出处理器，三个有界队列与精确终止计数 |
| `internal/crawler/config.go`、`options.go` | 新增 `BufferCap`（每缓冲内存上限）与 `TempDir`（溢写目录）配置 |
| `cmd/crawley/main.go` | 暴露 `-buffer-cap`、`-temp-dir` 两个命令行开关 |

## 3. 有界队列 `queue.Queue[T]`

- 内存部分是**定长环形数组** `ring []T`，容量在 `New` 时固定，**永不扩容**；
  `MemLen()` 恒满足 `≤ Cap()`。
- 环形写满后，首次溢写会把内存中的元素整体转存到 `TempDir` 下的临时 JSON 文件，
  此后的 `Push` 追加写盘，`Pop` 按 FIFO 读盘；溢写文件被读空后自动删除，队列回到
  纯内存模式，可反复溢写、循环复用。
- `Push` **从不等待消费者**：要么进内存，要么落盘，因此不会因拥塞而阻塞或超时丢元素；
  仅当临时存储本身失败时返回 `*SpillError`（包装 `create/write/read/remove` 各环节）。
- `Pop(ctx)` 在空队列上阻塞，支持 context 取消；队列关闭且排空后返回 `ok=false`。
- `Close()` 幂等，负责关闭文件句柄并删除溢写文件，保证无残留；另提供
  `WaitEmpty(ctx)` 供运行结束时等待完全排空。
- 并发安全：内部 `sync.Mutex` 保护环形与溢写状态，配合容量为 1 的 `notify` 唤醒频道。

## 4. 运行时架构（`runState`）

每次 `RunContext` 建立一个独立 `runState`，拥有且仅有三个有界缓冲（去重集合 `seen`
与 HTML/CSS/JS 解析器按要求**不计入**缓冲上限）：

```
                 解析产出链接
  worker 池  ───────────────────▶  discover（发现，有界+溢写）
  (固定 N 个)                          │ 协调器 drive：去重、过滤
      ▲                                ▼
      │                          pending（待抓，有界+溢写）
      └────────────────────────────────┘
                                       │ 合规 URL
                                       ▼
                                 output（输出，有界+溢写）──▶ 单一输出处理器 ─▶ 用户回调
```

### 4.1 固定并发，绝不为每个链接建 goroutine

- worker 数量固定为配置的 `Workers`（启动一次），外加**一个**输出处理器 goroutine、
  **一个**终止观察 goroutine；协调器跑在调用方 goroutine 上。
- 链接只在队列间流转，不触发新 goroutine。门控测试 `TestRunNoPerLinkGoroutine`
  通过 `runtime.Stack` 扫描，断言 400 页扇出时 worker/处理器 goroutine 峰值
  不超过 `workers+1`。

### 4.2 精确终止：active + queued 双计数器

这是保证「不丢链接、不死锁、正常结束」的关键。

- `active`：已入 pending 或正被 worker 处理的 URL 数。协调器入队时 `+1`，
  worker 处理完（且其产出的所有链接都已计入 discover）后 `-1`。
- `queued`：已写入 discover 但协调器尚未消费的发现结果数。生产者 push 前 `+1`，
  协调器 pop 后 `-1`。
- 仅当 `active==0 && queued==0` 时，经 `sync.Once` 关闭 `doneCh` 终止运行。

关键不变量：**worker 在释放自己的 active 槽位之前，必定已把本页解析出的全部链接
计入 queued**。因此一个正在解析大扇出页面的 worker，不可能把计数器误降到零；
缓冲中尚有未消费发现时也不可能提前结束。`takeQueued` 本身**不**触发终止判断，
协调器只在一个发现被完整处理（去重 / 入队 / 输出）之后才调用 `checkDone`，
杜绝了「输出尚未入队就收尾」的竞态。

### 4.3 去重与既有语义保持

- 仍用 FNV URL 哈希 + `set.Unordered[uint64]` 去重，规则与原版一致：同一 URL 只抓取、
  只输出一次（`TestRunDeduplicatesOutput` 覆盖）。
- 种子 URL 本身被抓取但不输出（与历史行为一致）。
- 完整保留：标签过滤、忽略规则、抓取深度、子域策略、robots 策略、目录策略
  （show/hide/only）、HEAD 预检、JS/CSS 链接抽取。原有 `crawler_test.go` 的全部
  行为测试在新运行时上保持原断言通过。

## 5. 取消语义（`RunContext`）

`Run(uri, cb)` 保留并委托给新增的 `RunContext(ctx, uri, cb)`。

- 取消后**不再发起新的 HTTP 请求、不再启动新的用户回调**：
  - worker 在取活后、发请求前检查 `r.ctx.Err()`，已取消则跳过请求；
  - 输出处理器在 `rootCtx` 已取消时只排空 output，不再调用回调。
- **在途请求与内部任务允许跑完**：已被 worker 取走的 URL 完成抓取与解析；
  HTTP 调用的 context 派生自运行 context（`context.WithTimeout(r.ctx, timeout)`），
  既保留超时上限，又能在取消时尽快收敛而不是干等满超时。
- **已经开始的用户回调不被打断**：`stop()` 对 output 的 `WaitEmpty` 不设超时，
  让正在执行的慢回调自然返回（`TestRunCancelDuringSlowCallback` 覆盖）。
- 收尾完成后返回 **context 错误**（`context.Canceled` / `DeadlineExceeded`）；
  若期间发生存储故障，则存储错误优先（见下）。

## 6. 临时存储故障与资源清理

- `newRun` 用 `os.MkdirTemp(TempDir, "crawley-*")` 建立独立工作目录，失败即包装为
  导出错误 `ErrStorage` 返回，运行不会带病启动。
- 运行中任何溢写 `create/write/read` 失败都会被记为首个 `ErrStorage` 并取消运行，
  已缓冲数据不再继续，最终由 `outcome()` 返回该错误。
- 所有终态（正常完成、取消、存储故障）都会经 `defer run.cleanup()`：关闭三个队列
  （关闭即删除溢写文件）并 `os.RemoveAll` 整个工作目录。门控测试断言故障与正常运行后
  临时目录均无残留。

## 7. 门控回归测试（`crawler_gate_test.go`、`queue/queue_test.go`）

| 测试 | 断言 |
| --- | --- |
| `TestRunOverflowGraphComplete` | 500 页、缓冲仅 32、8 worker、慢回调下，501 个唯一 URL 全部采集，含规范化根地址 |
| `TestRunBuffersBoundedMemory` | 三个队列 `Cap()==24`，运行期 `MemLen()` 永不超帽，结束后临时目录无残留 |
| `TestRunCancelStopsAndReturnsContextError` | 取消后返回 `context.Canceled`，返回后无在途 HTTP 泄漏 |
| `TestRunCancelDuringSlowCallback` | 取消时已开始的 200ms 回调能跑完 |
| `TestRunContextPreCanceled` | 启动前取消也能快速返回 context 错误并清理 |
| `TestRunStorageFailureReportsAndCleans` | 溢写目录不可用时返回 `ErrStorage`，且只剩预置阻挡文件、无其它残留 |
| `TestRunSpillFailureMidRunNoResidue` | 用同名目录占位使溢写确定性失败，返回 `ErrStorage` 且工作目录被清空 |
| `TestRunTempDirCleanupNormalRun` | 正常运行后临时目录为空 |
| `TestRunNoPerLinkGoroutine` | 大扇出下 worker/处理器 goroutine 峰值 ≤ `workers+1` |
| `TestRunRobotsAndSitemapNoDeadlock` | robots + sitemap 初始化与大扇出、慢回调并发，30s 内必完成 |
| `TestRunDeduplicatesOutput` | 重复发现的 URL 只抓取、只输出一次 |
| `queue` 包测试 | 内存 FIFO、溢写 FIFO、溢写↔内存往返、阻塞/取消、关闭唤醒、并发收发、各类溢写故障与无残留、最小容量 |

> 关于 `TestRunSpillFailureMidRunNoResidue`：早期版本曾在运行中 `RemoveAll` 临时目录
> 来诱发故障，但在 Linux 上已打开的溢写文件句柄在目录项被 unlink 后仍可正常读写，
> 故障并不必然发生（竞态）。现改为在每个溢写路径预置**同名目录**，使 `os.Create`
> 确定性返回 `EISDIR`（对 root 同样生效），故障可稳定复现。

## 8. 配置项

| 选项 | 默认 | 范围 | 说明 |
| --- | --- | --- | --- |
| `WithBufferCap` / `-buffer-cap` | 1024 | 16 ~ 65536 | 每个内存缓冲的元素上限，超出部分落盘 |
| `WithTempDir` / `-temp-dir` | 系统临时目录 | 任意可写目录 | 溢写文件所在目录，每次运行建立独立子目录 |

去重集合与解析器不属于上述缓冲，其规模由站点本身 URL 数量决定，不在此上限内
（符合需求约定）。

## 9. 测试环境与限制（重要）

本机未安装 gcc / C 工具链，**无法启用 cgo，因此无法运行 `-race` 检测器**
（竞态检测器依赖 cgo）。全部测试均以下述方式运行并通过：

```
CGO_ENABLED=0 go test -count=1 -vet=off -tags test ./...
```

作为补偿，并发正确性主要通过：精确的双计数器不变量、队列内部锁与唤醒协议、
以及基于 `runtime.Stack` 的 goroutine 计数门控测试来保障。建议在具备 gcc 的
环境中补跑一次项目 Makefile 默认的 `make code/test-all`（即
`CGO_ENABLED=1 go test -race …`）做最终确认。`gofmt`、`go vet`、`go build`
均已在本机通过；`golangci-lint` 因环境未安装未能运行，已对照 `.golangci.yml`
人工核查函数长度（≤80 行）、圈复杂度（≤15）与错误处理约定。
