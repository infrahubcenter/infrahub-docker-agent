package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
)

// dockerClient wraps the official Docker Engine SDK, talking to the local
// daemon over its Unix socket (client.FromEnv respects DOCKER_HOST if set,
// defaulting to unix:///var/run/docker.sock) -- this agent runs ON the VM
// itself, so it's a direct, reliable, structured API call rather than the
// backend's own SSH+CLI-text-parsing approach (see backend/internal/
// services/docker_parse.go) for VMs that don't have this agent installed.
type dockerClient struct {
	cli       *client.Client
	resources hostResourcesCache
}

func newDockerClient() (*dockerClient, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("create docker client: %w", err)
	}
	return &dockerClient{cli: cli}, nil
}

func (d *dockerClient) EngineVersion(ctx context.Context) (EngineVersionResult, error) {
	v, err := d.cli.ServerVersion(ctx)
	if err != nil {
		return EngineVersionResult{}, err
	}
	return EngineVersionResult{Version: v.Version, APIVersion: v.APIVersion, OSType: v.Os, Arch: v.Arch}, nil
}

func containerStatus(state string) string {
	switch strings.ToLower(state) {
	case "running":
		return "RUNNING"
	case "restarting":
		return "RESTARTING"
	case "created":
		return "CREATED"
	case "exited":
		return "EXITED"
	case "paused":
		return "PAUSED"
	case "dead":
		return "DEAD"
	case "removing":
		return "REMOVING"
	default:
		return "UNKNOWN"
	}
}

// ListContainers lists every container (running or not), with the same
// inventory fields the backend's SSH-based discovery already captures --
// a drop-in alternate producer, never a schema change.
func (d *dockerClient) ListContainers(ctx context.Context) ([]ContainerInfo, error) {
	containers, err := d.cli.ContainerList(ctx, container.ListOptions{All: true})
	if err != nil {
		return nil, err
	}
	items := make([]ContainerInfo, 0, len(containers))
	running := 0
	for _, c := range containers {
		name := strings.TrimPrefix(firstOrEmpty(c.Names), "/")
		info := ContainerInfo{
			ContainerID: c.ID,
			Name:        name,
			Image:       c.Image,
			Status:      containerStatus(c.State),
			State:       c.State,
			Command:     c.Command,
			CreatedAt:   time.Unix(c.Created, 0).UTC().Format(time.RFC3339),
		}
		// Inspect for restart count / health / precise started-at -- not
		// present on the list summary itself. One inspect per container per
		// discovery cycle, same batching discipline as the SSH path's own
		// `docker inspect id1 id2 ...` (just issued as N Engine API calls
		// instead of one CLI invocation, since the SDK has no batch-inspect).
		if detail, err := d.cli.ContainerInspect(ctx, c.ID); err == nil {
			info.RestartCount = detail.RestartCount
			if detail.State != nil {
				if detail.State.Health != nil {
					info.Health = strings.ToUpper(detail.State.Health.Status)
				}
				if detail.State.StartedAt != "" && detail.State.StartedAt != "0001-01-01T00:00:00Z" {
					info.StartedAt = detail.State.StartedAt
				}
			}
		} else {
			log.Printf("docker discovery: inspect failed for container %s, using basic info: %v", shortID(c.ID), err)
		}
		if info.Status == "RUNNING" {
			running++
		}
		items = append(items, info)
	}
	log.Printf("docker discovery: %d containers (%d running)", len(items), running)
	return items, nil
}

func firstOrEmpty(s []string) string {
	if len(s) == 0 {
		return ""
	}
	return s[0]
}

// shortID truncates a container ID to the 12-character form `docker ps`
// prints, for readable stdout logging only -- every API call above still
// uses the full ID.
func shortID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

// dockerNoMemoryLimitThreshold mirrors backend/internal/services/
// docker_parse.go's own sentinel exactly -- Docker reports a container
// with no real memory limit as an enormous (effectively "unbounded")
// number rather than zero/absent.
const dockerNoMemoryLimitThreshold = 1 << 62

// ContainerStats gets one point-in-time (non-streaming) stats sample for
// every currently-running container -- mirrors the SSH path's "one
// `docker stats --no-stream` call covers every container" mandate, just
// issued as one Engine API call per running container (the SDK's stats
// endpoint is inherently per-container; there is no Engine-side batch
// equivalent of `docker stats` with no arguments).
func (d *dockerClient) ContainerStats(ctx context.Context) ([]ContainerStatsResult, error) {
	containers, err := d.cli.ContainerList(ctx, container.ListOptions{})
	if err != nil {
		return nil, err
	}
	results := make([]ContainerStatsResult, 0, len(containers))
	for _, c := range containers {
		stats, err := d.oneContainerStats(ctx, c.ID)
		if err != nil {
			log.Printf("metrics collection: failed to read stats for container %s, skipping: %v", shortID(c.ID), err)
			continue
		}
		results = append(results, stats)
	}
	log.Printf("metrics collection: sampled %d/%d running containers", len(results), len(containers))
	return results, nil
}

func (d *dockerClient) oneContainerStats(ctx context.Context, containerID string) (ContainerStatsResult, error) {
	resp, err := d.cli.ContainerStatsOneShot(ctx, containerID)
	if err != nil {
		return ContainerStatsResult{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return ContainerStatsResult{}, err
	}
	var raw container.StatsResponse
	if err := json.Unmarshal(body, &raw); err != nil {
		return ContainerStatsResult{}, err
	}

	result := ContainerStatsResult{ContainerID: containerID, PIDs: int(raw.PidsStats.Current)}

	cpuDelta := float64(raw.CPUStats.CPUUsage.TotalUsage) - float64(raw.PreCPUStats.CPUUsage.TotalUsage)
	systemDelta := float64(raw.CPUStats.SystemUsage) - float64(raw.PreCPUStats.SystemUsage)
	numCPUs := float64(raw.CPUStats.OnlineCPUs)
	if numCPUs == 0 {
		numCPUs = float64(len(raw.CPUStats.CPUUsage.PercpuUsage))
	}
	if systemDelta > 0 && cpuDelta > 0 && numCPUs > 0 {
		result.CPUPercent = (cpuDelta / systemDelta) * numCPUs * 100
	}

	result.MemoryUsageBytes = int64(raw.MemoryStats.Usage)
	if raw.MemoryStats.Limit > 0 && raw.MemoryStats.Limit < dockerNoMemoryLimitThreshold {
		result.MemoryLimitBytes = int64(raw.MemoryStats.Limit)
		result.HasMemoryLimit = true
		result.MemoryPercent = float64(raw.MemoryStats.Usage) / float64(raw.MemoryStats.Limit) * 100
	}

	for _, net := range raw.Networks {
		result.NetworkRxBytes += int64(net.RxBytes)
		result.NetworkTxBytes += int64(net.TxBytes)
	}

	for _, entry := range raw.BlkioStats.IoServiceBytesRecursive {
		switch strings.ToLower(entry.Op) {
		case "read":
			result.BlockReadBytes += int64(entry.Value)
		case "write":
			result.BlockWriteBytes += int64(entry.Value)
		}
	}

	return result, nil
}

// HostResources answers the "host_resources" command -- the Docker Host's
// full image/volume/build-cache inventory (one `docker system df` /
// "/system/df" call, the same source `docker system df` itself uses) plus
// networks (a separate NetworkList call, since networks carry no size).
func (d *dockerClient) HostResources(ctx context.Context) (HostResourcesResult, error) {
	du, err := d.cli.DiskUsage(ctx, types.DiskUsageOptions{})
	if err != nil {
		return HostResourcesResult{}, fmt.Errorf("disk usage: %w", err)
	}
	networks, err := d.cli.NetworkList(ctx, network.ListOptions{})
	if err != nil {
		return HostResourcesResult{}, fmt.Errorf("list networks: %w", err)
	}

	result := HostResourcesResult{
		Images:     make([]ImageItem, 0, len(du.Images)),
		Volumes:    make([]VolumeItem, 0, len(du.Volumes)),
		Networks:   make([]NetworkItem, 0, len(networks)),
		BuildCache: make([]BuildCacheItem, 0, len(du.BuildCache)),
	}

	for _, img := range du.Images {
		if img == nil {
			continue
		}
		item := ImageItem{ID: img.ID, RepoTags: img.RepoTags, SizeBytes: img.Size, Containers: img.Containers}
		if img.Created > 0 {
			item.CreatedAt = time.Unix(img.Created, 0).UTC().Format(time.RFC3339)
		}
		result.Images = append(result.Images, item)
		result.TotalImagesSizeBytes += img.Size
	}

	for _, vol := range du.Volumes {
		if vol == nil {
			continue
		}
		item := VolumeItem{Name: vol.Name, Driver: vol.Driver, Mountpoint: vol.Mountpoint, CreatedAt: vol.CreatedAt}
		if vol.UsageData != nil && vol.UsageData.Size >= 0 {
			size := vol.UsageData.Size
			item.SizeBytes = &size
			result.TotalVolumesSizeBytes += size
		}
		result.Volumes = append(result.Volumes, item)
	}

	for _, n := range networks {
		result.Networks = append(result.Networks, NetworkItem{
			ID: n.ID, Name: n.Name, Driver: n.Driver, Scope: n.Scope, Containers: len(n.Containers),
		})
	}

	for _, bc := range du.BuildCache {
		if bc == nil {
			continue
		}
		item := BuildCacheItem{
			ID: bc.ID, Type: bc.Type, Description: bc.Description, SizeBytes: bc.Size, InUse: bc.InUse, Shared: bc.Shared,
		}
		if bc.LastUsedAt != nil {
			item.LastUsedAt = bc.LastUsedAt.UTC().Format(time.RFC3339)
		}
		result.BuildCache = append(result.BuildCache, item)
		result.TotalBuildCacheSizeBytes += bc.Size
	}

	log.Printf("host resources: %d images (%s), %d volumes (%s), %d networks, %d build cache entries (%s)",
		len(result.Images), formatBytesLog(result.TotalImagesSizeBytes),
		len(result.Volumes), formatBytesLog(result.TotalVolumesSizeBytes),
		len(result.Networks), len(result.BuildCache), formatBytesLog(result.TotalBuildCacheSizeBytes))
	return result, nil
}

// formatBytesLog is a tiny human-readable formatter for this file's own
// stdout logging only -- the wire protocol above always carries raw
// bytes; formatting for display is the backend/frontend's job.
func formatBytesLog(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%dB", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

// FetchLogsSince gets a bounded, non-follow batch of log lines newer than
// since -- used by the background log-capture cycle (never the live-tail
// stream, which uses StreamLogs below). Mirrors K8sService.FetchLogsSince's
// role exactly.
func (d *dockerClient) FetchLogsSince(ctx context.Context, containerID string, since time.Time) (string, error) {
	opts := container.LogsOptions{ShowStdout: true, ShowStderr: true, Timestamps: true}
	if !since.IsZero() {
		opts.Since = since.UTC().Format(time.RFC3339Nano)
	}
	reader, err := d.cli.ContainerLogs(ctx, containerID, opts)
	if err != nil {
		return "", err
	}
	defer reader.Close()
	return demuxDockerLogs(reader)
}

// StreamLogs follows a container's stdout/stderr live, invoking onLine
// once per line, until ctx is cancelled or the stream ends.
func (d *dockerClient) StreamLogs(ctx context.Context, containerID string, onLine func(string)) error {
	reader, err := d.cli.ContainerLogs(ctx, containerID, container.LogsOptions{
		ShowStdout: true, ShowStderr: true, Timestamps: true, Follow: true, Tail: "200",
	})
	if err != nil {
		return err
	}
	defer reader.Close()
	return streamDemuxDockerLogs(ctx, reader, onLine)
}
