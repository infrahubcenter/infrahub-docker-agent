// Command docker-agent is InfraHub's per-VM Docker agent: installed once
// (by InfraHub itself, over the same SSH connection already used to
// manage the VM -- see backend/internal/services/docker_agent_install.go)
// on a VM you want richer Docker monitoring/logs for, given a backend URL
// and a bearer token, it dials OUT to InfraHub over a WebSocket -- mirrors
// /k8s-agent's architecture exactly (see that binary's own main.go for the
// full rationale of dialing out vs. inbound access). Talks to the local
// Docker Engine directly over its Unix socket, never over SSH+CLI --
// see docker.go.
//
// This is additive, not a replacement: a VM with no agent installed keeps
// working exactly as before (backend/internal/services/docker_metrics_
// service.go's SSH+CLI polling); a VM with the agent installed gets this
// instead, automatically, with no change to any downstream table, cache,
// or UI.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	backendURL := os.Getenv("INFRAHUB_BACKEND_URL")
	token := os.Getenv("INFRAHUB_AGENT_TOKEN")
	if backendURL == "" || token == "" {
		log.Fatal("INFRAHUB_BACKEND_URL and INFRAHUB_AGENT_TOKEN must both be set")
	}
	log.Printf("starting docker-agent: backend=%s token=provided", backendURL)

	docker, err := newDockerClient()
	if err != nil {
		log.Fatalf("docker client: %v", err)
	}
	log.Print("docker client initialized")
	docker.WarmHostResources()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	const maxBackoff = 30 * time.Second
	backoff := time.Second
	for ctx.Err() == nil {
		connectedAt := time.Now()
		if err := runOnce(ctx, backendURL, token, docker); err != nil {
			log.Printf("connection ended: %v", err)
		}
		if ctx.Err() != nil {
			return
		}
		// A connection that stayed up for a while is treated as healthy --
		// reset the backoff rather than let one brief, long-ago blip keep
		// slowing every future reconnect.
		if time.Since(connectedAt) > maxBackoff {
			backoff = time.Second
		}
		log.Printf("reconnecting in %s", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
		}
	}
}

// pongWait/pingPeriod: exact values proven this session against a real
// NAT path (Docker Desktop Windows) for the K8s agent -- see that
// binary's main.go for the full derivation. Reused verbatim rather than
// re-derived: several ping attempts per window (not one near the
// deadline) tolerates network jitter while still bounding worst-case
// dead-connection detection time to pongWait.
const pongWait = 90 * time.Second
const pingPeriod = 15 * time.Second

// runOnce holds one connection open until it fails or ctx is cancelled,
// dispatching every inbound command to its own goroutine (engine_version/
// list_containers/container_stats/fetch_logs_since answer once and
// return; stream_logs runs until stop_stream or the connection closes).
func runOnce(ctx context.Context, backendURL, token string, docker *dockerClient) error {
	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	log.Printf("connecting to InfraHub at %s", backendURL)
	conn, _, err := dialer.DialContext(ctx, backendURL, http.Header{"Authorization": {"Bearer " + token}})
	if err != nil {
		return err
	}
	defer conn.Close()
	log.Println("connected to InfraHub")

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-connCtx.Done()
		_ = conn.Close()
	}()

	var writeMu sync.Mutex
	var activeStreams sync.Map // command ID -> context.CancelFunc, for in-flight stream_logs commands

	_ = conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
	go pingLoop(connCtx, conn, &writeMu, cancel)

	for {
		var cmd Command
		if err := conn.ReadJSON(&cmd); err != nil {
			// Every stream this connection was running dies along with it --
			// cancel them so their goroutines don't leak past the connection
			// they were writing to.
			activeStreams.Range(func(_, v any) bool {
				v.(context.CancelFunc)()
				return true
			})
			return err
		}
		_ = conn.SetReadDeadline(time.Now().Add(pongWait))
		go handleCommand(connCtx, conn, &writeMu, docker, cmd, &activeStreams)
	}
}

// pingLoop is this connection's active liveness check -- mirrors
// k8s-agent/main.go's pingLoop exactly (see backend/internal/services/
// docker_agent_hub.go's readLoop for the matching server-side override).
func pingLoop(ctx context.Context, conn *websocket.Conn, writeMu *sync.Mutex, cancel context.CancelFunc) {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			writeMu.Lock()
			err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(10*time.Second))
			writeMu.Unlock()
			if err != nil {
				cancel()
				return
			}
		}
	}
}

func handleCommand(ctx context.Context, conn *websocket.Conn, writeMu *sync.Mutex, docker *dockerClient, cmd Command, activeStreams *sync.Map) {
	switch cmd.Type {
	case CmdEngineVersion:
		version, err := docker.EngineVersion(ctx)
		if err != nil {
			log.Printf("engine_version query failed: %v", err)
		}
		sendResult(conn, writeMu, cmd.ID, version, err)

	case CmdListContainers:
		containers, err := docker.ListContainers(ctx)
		if err != nil {
			log.Printf("container discovery failed: %v", err)
		}
		sendResult(conn, writeMu, cmd.ID, containers, err)

	case CmdContainerStats:
		stats, err := docker.ContainerStats(ctx)
		if err != nil {
			log.Printf("metrics collection failed: %v", err)
		}
		sendResult(conn, writeMu, cmd.ID, stats, err)

	case CmdHostResources:
		resources, err := docker.CachedHostResources(ctx)
		if err != nil {
			log.Printf("host resources query failed: %v", err)
		}
		sendResult(conn, writeMu, cmd.ID, resources, err)

	case CmdHostSystemMetrics:
		metrics, err := docker.HostSystemMetrics(ctx)
		if err != nil {
			log.Printf("host system metrics query failed: %v", err)
		}
		sendResult(conn, writeMu, cmd.ID, metrics, err)

	case CmdFetchLogsSince:
		var since time.Time
		if cmd.Since != "" {
			since, _ = time.Parse(time.RFC3339Nano, cmd.Since)
		}
		output, err := docker.FetchLogsSince(ctx, cmd.ContainerID, since)
		if err != nil {
			log.Printf("fetch_logs_since failed for container %s: %v", shortID(cmd.ContainerID), err)
		} else {
			log.Printf("fetch_logs_since: sent %d lines for container %s", countLines(output), shortID(cmd.ContainerID))
		}
		sendResult(conn, writeMu, cmd.ID, map[string]string{"output": output}, err)

	case CmdStreamLogs:
		streamCtx, cancel := context.WithCancel(ctx)
		activeStreams.Store(cmd.ID, cancel)
		defer func() {
			activeStreams.Delete(cmd.ID)
			cancel()
		}()
		log.Printf("log stream started for container %s", shortID(cmd.ContainerID))
		lines := 0
		err := docker.StreamLogs(streamCtx, cmd.ContainerID, func(line string) {
			lines++
			writeMu.Lock()
			_ = conn.WriteJSON(Message{ID: cmd.ID, Type: MsgLogLine, Line: line})
			writeMu.Unlock()
		})
		writeMu.Lock()
		if err != nil && streamCtx.Err() == nil {
			_ = conn.WriteJSON(Message{ID: cmd.ID, Type: MsgError, Message: "log stream ended: " + err.Error()})
		} else {
			_ = conn.WriteJSON(Message{ID: cmd.ID, Type: MsgDone})
		}
		writeMu.Unlock()
		if err != nil && streamCtx.Err() == nil {
			log.Printf("log stream ended for container %s: forwarded %d lines, error: %v", shortID(cmd.ContainerID), lines, err)
		} else {
			log.Printf("log stream ended for container %s: forwarded %d lines", shortID(cmd.ContainerID), lines)
		}

	case CmdStopStream:
		if v, ok := activeStreams.Load(cmd.ID); ok {
			v.(context.CancelFunc)()
			activeStreams.Delete(cmd.ID)
			log.Printf("log stream stopped by request: command %s", cmd.ID)
		}

	default:
		// An unrecognized command means this agent binary predates a
		// backend that's added new commands -- fail fast and legibly
		// (rather than the backend silently timing out waiting for a
		// response that will never come) so a version-skew rollout is
		// obvious in the logs instead of a mysterious hang.
		log.Printf("received unknown command %q -- agent may need upgrading", cmd.Type)
		writeMu.Lock()
		_ = conn.WriteJSON(Message{ID: cmd.ID, Type: MsgError, Message: "unknown command \"" + string(cmd.Type) + "\" -- agent may need upgrading"})
		writeMu.Unlock()
	}
}

// countLines returns the number of newline-terminated lines in a
// FetchLogsSince-shaped blob, for a compact "sent N lines" log line --
// purely for this agent's own stdout, never sent over the wire.
func countLines(s string) int {
	if s == "" {
		return 0
	}
	return strings.Count(s, "\n")
}

func sendResult(conn *websocket.Conn, writeMu *sync.Mutex, id string, data any, err error) {
	writeMu.Lock()
	defer writeMu.Unlock()
	if err != nil {
		_ = conn.WriteJSON(Message{ID: id, Type: MsgError, Message: err.Error()})
		return
	}
	raw, marshalErr := json.Marshal(data)
	if marshalErr != nil {
		_ = conn.WriteJSON(Message{ID: id, Type: MsgError, Message: "internal error encoding response"})
		return
	}
	_ = conn.WriteJSON(Message{ID: id, Type: MsgResult, Data: raw})
}
