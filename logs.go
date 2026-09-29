package main

import (
	"bufio"
	"context"
	"io"
	"log"
	"strings"

	"github.com/docker/docker/pkg/stdcopy"
)

// demuxDockerLogs reads a non-TTY container's multiplexed stdout/stderr
// log stream (Docker's 8-byte-header framing -- see
// github.com/docker/docker/pkg/stdcopy) into one plain-text blob, newline
// per line, matching the shape the SSH path's `docker logs` CLI output
// already produces for downstream parsing (see backend/internal/services/
// docker_log_capture.go).
func demuxDockerLogs(r io.Reader) (string, error) {
	var out strings.Builder
	pr, pw := io.Pipe()
	go func() {
		_, err := stdcopy.StdCopy(pw, pw, r)
		pw.CloseWithError(err)
	}()
	scanner := bufio.NewScanner(pr)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		out.WriteString(scanner.Text())
		out.WriteByte('\n')
	}
	if err := scanner.Err(); err != nil {
		log.Printf("fetch_logs_since: log read ended early: %v", err)
	}
	return out.String(), nil
}

// streamDemuxDockerLogs is demuxDockerLogs' live-tail counterpart --
// invokes onLine once per complete line as it arrives, until ctx is
// cancelled or the stream ends.
func streamDemuxDockerLogs(ctx context.Context, r io.Reader, onLine func(string)) error {
	pr, pw := io.Pipe()
	go func() {
		_, err := stdcopy.StdCopy(pw, pw, r)
		pw.CloseWithError(err)
	}()
	go func() {
		<-ctx.Done()
		pr.Close()
	}()
	scanner := bufio.NewScanner(pr)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		onLine(scanner.Text())
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return scanner.Err()
}
