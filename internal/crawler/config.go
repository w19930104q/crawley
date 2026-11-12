package crawler

import (
	"fmt"
	"strings"
	"time"

	"github.com/s0rg/crawley/internal/client"
)

const (
	minDepth   = -1
	minWorkers = 1
	maxWorkers = 64
	minDelay   = time.Duration(0)
	minTimeout = time.Second
	maxTimeout = time.Minute * 10

	// minBufferCap is the smallest allowed in-memory capacity of the
	// discovery, pending and output buffers.
	minBufferCap = 16
	// maxBufferCap bounds the per-buffer in-memory capacity.
	maxBufferCap = 1 << 16
	// defaultBufferCap is used when no explicit capacity is configured.
	defaultBufferCap = 1024
)

type config struct {
	AlowedTags []string
	Ignored    []string
	Client     client.Config
	Delay      time.Duration
	Depth      int
	Robots     RobotsPolicy
	Dirs       DirsPolicy
	Brute      bool
	NoHEAD     bool
	ScanJS     bool
	ScanCSS    bool
	Subdomains bool
	// BufferCap is the maximum number of items each in-memory buffer
	// (discovery, pending and output) may hold; overflow spills to TempDir.
	BufferCap int
	// TempDir is the directory for temporary spill files.
	TempDir string
}

func (c *config) String() (rv string) {
	var sb strings.Builder

	fmt.Fprintf(&sb, "workers: %d depth: %d timeout: %s", c.Client.Workers, c.Depth, c.Client.Timeout)

	if c.Brute {
		sb.WriteString(" brute: on")
	}

	if c.Delay > 0 {
		fmt.Fprintf(&sb, " delay: %s", c.Delay)
	}

	if c.ScanJS {
		sb.WriteString(" +js")
	}

	if c.ScanCSS {
		sb.WriteString(" +css")
	}

	if c.Subdomains {
		sb.WriteString(" +subdomains")
	}

	return sb.String()
}

func (c *config) validate() {
	c.Client.Workers = min(maxWorkers, max(minWorkers, c.Client.Workers))
	c.Client.Timeout = min(maxTimeout, max(minTimeout, c.Client.Timeout))
	c.Delay = max(minDelay, c.Delay)
	c.Depth = max(minDepth, c.Depth)
	c.BufferCap = min(maxBufferCap, max(minBufferCap, c.BufferCap))
}
