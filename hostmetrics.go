package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

// The Docker HOST machine's own OS-level metrics -- read directly from
// /proc and a statfs of the host's real root, both bind-mounted
// read-only into this container by DockerAgentRunCommand:
//
//	-v /proc:/host/proc:ro -v /:/host/root:ro
//
// Never available from the Docker Engine API itself: Info() only reports
// static capacity (NCPU/MemTotal), never live usage, and has no load
// average or disk-free at all -- this is the one thing genuinely
// impossible to get any other way while staying "just Docker" (no SSH,
// no separate VM Agent, matching how Docker Hosts have no VM record and
// no SSH access at all by design).
const (
	hostProcPath = "/host/proc"
	hostRootPath = "/host/root"
)

// cpuSample is one /proc/stat "cpu " line's cumulative jiffie counters.
type cpuSample struct {
	idle, total uint64
}

// previousCPUSample is package-level, deliberately -- CPU% is a delta
// between two points in time, and this agent's own command/response
// protocol has no notion of "the previous poll" otherwise. Guarded by a
// mutex since command dispatch isn't guaranteed single-threaded (see
// main.go's per-connection goroutines); a stale sample is harmless (worst
// case one poll reports against a slightly older baseline), so a plain
// Mutex (not needing per-key granularity) is enough -- there's only ever
// one host to sample.
var (
	previousCPUSampleMu sync.Mutex
	previousCPUSample   *cpuSample
)

func readHostCPUSample() (cpuSample, error) {
	f, err := os.Open(hostProcPath + "/stat")
	if err != nil {
		return cpuSample{}, fmt.Errorf("open /proc/stat: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	if !scanner.Scan() {
		return cpuSample{}, fmt.Errorf("empty /proc/stat")
	}
	fields := strings.Fields(scanner.Text())
	// "cpu  user nice system idle iowait irq softirq steal guest guest_nice"
	if len(fields) < 5 || fields[0] != "cpu" {
		return cpuSample{}, fmt.Errorf("unexpected /proc/stat format: %q", scanner.Text())
	}
	values := make([]uint64, 0, len(fields)-1)
	for _, s := range fields[1:] {
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return cpuSample{}, fmt.Errorf("parse /proc/stat field %q: %w", s, err)
		}
		values = append(values, v)
	}
	var total uint64
	for _, v := range values {
		total += v
	}
	idle := values[3] // idle
	if len(values) > 4 {
		idle += values[4] // + iowait
	}
	return cpuSample{idle: idle, total: total}, nil
}

// readHostCPUCoreCount counts the per-core "cpuN" lines in /proc/stat
// (cpu0, cpu1, ... -- distinct from the first "cpu " line, which is the
// all-cores aggregate readHostCPUSample reads). Logical cores (as
// nproc/`docker info`'s NCPU report), not physical -- matches what "14.9%
// of 4 cores" means to an admin comparing this against their own
// instance size.
func readHostCPUCoreCount() (int, error) {
	f, err := os.Open(hostProcPath + "/stat")
	if err != nil {
		return 0, fmt.Errorf("open /proc/stat: %w", err)
	}
	defer f.Close()

	count := 0
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "cpu") {
			break // the per-core "cpuN" lines are always first and contiguous
		}
		if len(line) > 3 && line[3] >= '0' && line[3] <= '9' {
			count++
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("scan /proc/stat: %w", err)
	}
	if count == 0 {
		return 0, fmt.Errorf("no per-core cpuN lines found in /proc/stat")
	}
	return count, nil
}

// cpuPercentSinceLastCall returns 0 on this agent's first-ever call (no
// prior sample to diff against) -- self-corrects on the next poll, same
// as every other "needs two samples" rate this app already computes
// (e.g. the VM monitoring scheduler's own CPU delta math).
func cpuPercentSinceLastCall() (float64, error) {
	current, err := readHostCPUSample()
	if err != nil {
		return 0, err
	}
	previousCPUSampleMu.Lock()
	previous := previousCPUSample
	previousCPUSample = &current
	previousCPUSampleMu.Unlock()

	if previous == nil || current.total <= previous.total {
		return 0, nil
	}
	totalDelta := current.total - previous.total
	idleDelta := current.idle - previous.idle
	if idleDelta > totalDelta {
		return 0, nil
	}
	return (float64(totalDelta-idleDelta) / float64(totalDelta)) * 100, nil
}

func readHostMemory() (totalBytes, usedBytes int64, err error) {
	f, err := os.Open(hostProcPath + "/meminfo")
	if err != nil {
		return 0, 0, fmt.Errorf("open /proc/meminfo: %w", err)
	}
	defer f.Close()

	var totalKB, availableKB int64
	haveTotal, haveAvailable := false, false
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		switch fields[0] {
		case "MemTotal:":
			totalKB, err = strconv.ParseInt(fields[1], 10, 64)
			if err != nil {
				return 0, 0, fmt.Errorf("parse MemTotal: %w", err)
			}
			haveTotal = true
		case "MemAvailable:":
			availableKB, err = strconv.ParseInt(fields[1], 10, 64)
			if err != nil {
				return 0, 0, fmt.Errorf("parse MemAvailable: %w", err)
			}
			haveAvailable = true
		}
		if haveTotal && haveAvailable {
			break
		}
	}
	if !haveTotal || !haveAvailable {
		return 0, 0, fmt.Errorf("MemTotal/MemAvailable not found in /proc/meminfo")
	}
	totalBytes = totalKB * 1024
	usedBytes = totalBytes - availableKB*1024
	return totalBytes, usedBytes, nil
}

func readHostLoadAvg() (load1, load5, load15 float64, err error) {
	data, err := os.ReadFile(hostProcPath + "/loadavg")
	if err != nil {
		return 0, 0, 0, fmt.Errorf("open /proc/loadavg: %w", err)
	}
	fields := strings.Fields(string(data))
	if len(fields) < 3 {
		return 0, 0, 0, fmt.Errorf("unexpected /proc/loadavg format: %q", string(data))
	}
	load1, err = strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("parse load1: %w", err)
	}
	load5, err = strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("parse load5: %w", err)
	}
	load15, err = strconv.ParseFloat(fields[2], 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("parse load15: %w", err)
	}
	return load1, load5, load15, nil
}

// dockerDataPath is the host path whose filesystem Host Disk reports: the
// one holding Docker's data root (images, volumes, build cache), read via
// the /host/root bind mount. On a typical Linux server that is the same
// disk as /; on Docker Desktop, / is a ~3 GB internal overlay while
// /var/lib/docker sits on the real (~1 TB) data disk, so statting / alone
// showed "516 KB of 3.3 GB". Resolved once from `docker info`.
var (
	dockerDataPathOnce sync.Once
	dockerDataPath     = hostRootPath
)

func (d *dockerClient) resolveDockerDataPath(ctx context.Context) string {
	dockerDataPathOnce.Do(func() {
		info, err := d.cli.Info(ctx)
		if err != nil || info.DockerRootDir == "" {
			return
		}
		candidate := hostRootPath + info.DockerRootDir
		var stat syscall.Statfs_t
		if syscall.Statfs(candidate, &stat) == nil {
			dockerDataPath = candidate
		}
	})
	return dockerDataPath
}

func readHostDisk(path string) (totalBytes, usedBytes, availableBytes int64, err error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, 0, 0, fmt.Errorf("statfs %s: %w", path, err)
	}
	blockSize := uint64(stat.Bsize)
	total := stat.Blocks * blockSize
	available := stat.Bavail * blockSize
	free := stat.Bfree * blockSize
	used := total - free
	return int64(total), int64(used), int64(available), nil
}

// HostSystemMetrics reads the Docker HOST machine's own CPU/memory/load/
// disk -- see the package-level doc comment above for the two required
// bind mounts. Never fabricated: a genuine read failure (e.g. the bind
// mount is missing because the agent was installed before this feature
// existed) surfaces as an error, not a zeroed/fake result.
func (d *dockerClient) HostSystemMetrics(ctx context.Context) (HostSystemMetricsResult, error) {
	cpuPercent, err := cpuPercentSinceLastCall()
	if err != nil {
		return HostSystemMetricsResult{}, fmt.Errorf("read host cpu: %w", err)
	}
	cpuCores, err := readHostCPUCoreCount()
	if err != nil {
		return HostSystemMetricsResult{}, fmt.Errorf("read host cpu core count: %w", err)
	}
	memTotal, memUsed, err := readHostMemory()
	if err != nil {
		return HostSystemMetricsResult{}, fmt.Errorf("read host memory: %w", err)
	}
	load1, load5, load15, err := readHostLoadAvg()
	if err != nil {
		return HostSystemMetricsResult{}, fmt.Errorf("read host load average: %w", err)
	}
	diskTotal, diskUsed, diskAvailable, err := readHostDisk(d.resolveDockerDataPath(ctx))
	if err != nil {
		return HostSystemMetricsResult{}, fmt.Errorf("read host disk: %w", err)
	}
	return HostSystemMetricsResult{
		CPUPercent:         cpuPercent,
		CPUCores:           cpuCores,
		MemoryTotalBytes:   memTotal,
		MemoryUsedBytes:    memUsed,
		LoadAvg1:           load1,
		LoadAvg5:           load5,
		LoadAvg15:          load15,
		DiskTotalBytes:     diskTotal,
		DiskUsedBytes:      diskUsed,
		DiskAvailableBytes: diskAvailable,
	}, nil
}
