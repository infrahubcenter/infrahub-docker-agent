package main

import "encoding/json"

type CommandType string

const (
	CmdEngineVersion     CommandType = "engine_version"
	CmdListContainers    CommandType = "list_containers"
	CmdContainerStats    CommandType = "container_stats"
	CmdFetchLogsSince    CommandType = "fetch_logs_since"
	CmdStreamLogs        CommandType = "stream_logs"
	CmdStopStream        CommandType = "stop_stream"
	CmdHostResources     CommandType = "host_resources"
	CmdHostSystemMetrics CommandType = "host_system_metrics"
)

type Command struct {
	ID          string      `json:"id"`
	Type        CommandType `json:"type"`
	ContainerID string      `json:"container_id,omitempty"`
	Since       string      `json:"since,omitempty"`
}

type MessageType string

const (
	MsgResult  MessageType = "result"
	MsgLogLine MessageType = "log_line"
	MsgDone    MessageType = "done"
	MsgError   MessageType = "error"
)

type Message struct {
	ID      string          `json:"id"`
	Type    MessageType     `json:"type"`
	Data    json.RawMessage `json:"data,omitempty"`
	Line    string          `json:"line,omitempty"`
	Message string          `json:"message,omitempty"`
}

// EngineVersionResult is what "engine_version" returns -- the simplest
// possible round trip proving the agent is up and can reach the local
// Docker Engine over its socket.
type EngineVersionResult struct {
	Version    string `json:"version"`
	APIVersion string `json:"api_version"`
	OSType     string `json:"os_type"`
	Arch       string `json:"arch"`
}

// ContainerInfo is what "list_containers" returns, one entry per
// container -- a small, stable, safe-to-serialize shape (never a raw
// Docker SDK type), matching AgentPodInfo's own discipline in the K8s
// agent's protocol.
type ContainerInfo struct {
	ContainerID  string `json:"container_id"`
	Name         string `json:"name"`
	Image        string `json:"image"`
	Status       string `json:"status"` // RUNNING/RESTARTING/CREATED/EXITED/PAUSED/DEAD/REMOVING/UNKNOWN
	State        string `json:"state"`
	Health       string `json:"health,omitempty"`
	Command      string `json:"command,omitempty"`
	RestartCount int    `json:"restart_count"`
	CreatedAt    string `json:"created_at,omitempty"` // RFC3339
	StartedAt    string `json:"started_at,omitempty"` // RFC3339
}

// ContainerStatsResult is what "container_stats" returns -- one entry per
// currently-running container, matching the exact field set/semantics
// backend/internal/services/docker_parse.go's ContainerStats already
// uses, so this can be a drop-in alternate producer for the existing
// docker_container_metric_snapshots pipeline (no downstream schema/UI
// change needed).
type ContainerStatsResult struct {
	ContainerID      string  `json:"container_id"`
	CPUPercent       float64 `json:"cpu_percent"`
	MemoryUsageBytes int64   `json:"memory_usage_bytes"`
	MemoryLimitBytes int64   `json:"memory_limit_bytes"`
	HasMemoryLimit   bool    `json:"has_memory_limit"`
	MemoryPercent    float64 `json:"memory_percent"`
	NetworkRxBytes   int64   `json:"network_rx_bytes"`
	NetworkTxBytes   int64   `json:"network_tx_bytes"`
	BlockReadBytes   int64   `json:"block_read_bytes"`
	BlockWriteBytes  int64   `json:"block_write_bytes"`
	PIDs             int     `json:"pids"`
}

// HostResourcesResult is what "host_resources" returns -- the Docker
// Host's full image/volume/network/build-cache inventory with storage
// sizes, sourced from one `docker system df` (Engine API "/system/df")
// call for images/volumes/build-cache (the same numbers `docker system
// df` itself prints) plus one NetworkList call, since networks aren't
// part of disk-usage accounting. Never fabricated: a size Docker itself
// doesn't report (e.g. a volume whose usage wasn't computed) is left nil,
// not defaulted to 0.
type HostResourcesResult struct {
	Images                   []ImageItem      `json:"images"`
	Volumes                  []VolumeItem     `json:"volumes"`
	Networks                 []NetworkItem    `json:"networks"`
	BuildCache               []BuildCacheItem `json:"build_cache"`
	TotalImagesSizeBytes     int64            `json:"total_images_size_bytes"`
	TotalVolumesSizeBytes    int64            `json:"total_volumes_size_bytes"`
	TotalBuildCacheSizeBytes int64            `json:"total_build_cache_size_bytes"`
}

// ImageItem is one entry in HostResourcesResult.Images.
type ImageItem struct {
	ID         string   `json:"id"`
	RepoTags   []string `json:"repo_tags,omitempty"`
	SizeBytes  int64    `json:"size_bytes"`
	Containers int64    `json:"containers"`
	CreatedAt  string   `json:"created_at,omitempty"` // RFC3339
}

// VolumeItem is one entry in HostResourcesResult.Volumes.
type VolumeItem struct {
	Name       string `json:"name"`
	Driver     string `json:"driver"`
	Mountpoint string `json:"mountpoint,omitempty"`
	SizeBytes  *int64 `json:"size_bytes,omitempty"` // nil when Docker didn't compute usage for this volume
	CreatedAt  string `json:"created_at,omitempty"` // RFC3339
}

// NetworkItem is one entry in HostResourcesResult.Networks.
type NetworkItem struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Driver     string `json:"driver"`
	Scope      string `json:"scope"`
	Containers int    `json:"containers"`
}

// BuildCacheItem is one entry in HostResourcesResult.BuildCache.
type BuildCacheItem struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	Description string `json:"description,omitempty"`
	SizeBytes   int64  `json:"size_bytes"`
	InUse       bool   `json:"in_use"`
	Shared      bool   `json:"shared"`
	LastUsedAt  string `json:"last_used_at,omitempty"` // RFC3339
}

// HostSystemMetricsResult is what "host_system_metrics" returns -- the
// Docker HOST machine's own OS-level resource usage (CPU%/memory/load/
// disk), never available from the Docker Engine API itself (Info() only
// reports static capacity -- NCPU/MemTotal -- never live usage, and has
// no load average or disk-free at all). Read directly from /proc and a
// statfs of the host's real root filesystem, both bind-mounted read-only
// into this container by DockerAgentRunCommand (see hostmetrics.go) --
// this is the host machine itself, distinct from any single container's
// own cgroup usage. CPUPercent is 0 on this agent's first-ever call
// (needs two samples to compute a delta) and self-corrects a poll later.
type HostSystemMetricsResult struct {
	CPUPercent         float64 `json:"cpu_percent"`
	CPUCores           int     `json:"cpu_cores"`
	MemoryTotalBytes   int64   `json:"memory_total_bytes"`
	MemoryUsedBytes    int64   `json:"memory_used_bytes"`
	LoadAvg1           float64 `json:"load_avg_1"`
	LoadAvg5           float64 `json:"load_avg_5"`
	LoadAvg15          float64 `json:"load_avg_15"`
	DiskTotalBytes     int64   `json:"disk_total_bytes"`
	DiskUsedBytes      int64   `json:"disk_used_bytes"`
	DiskAvailableBytes int64   `json:"disk_available_bytes"`
}
