package main

import (
	"context"
	"log"
	"sync"
	"time"
)

// hostResourcesCache keeps the last `docker system df`-equivalent result
// (HostResources) so the host_resources command answers immediately.
// Docker's DiskUsage walks every image layer, volume and build-cache
// record; on a busy host (tens of GB of images/cache) it alone takes
// ~9s, which ran past the backend's per-command timeout and left the
// dashboard's Images/Volumes/Networks/Build Cache tiles empty. The result
// is refreshed in the background once it is older than
// hostResourcesMaxAge; only the very first call waits for a live read.
type hostResourcesCache struct {
	mu       sync.Mutex
	result   *HostResourcesResult
	at       time.Time
	lastErr  error
	inflight chan struct{}
}

const (
	hostResourcesMaxAge         = 60 * time.Second
	hostResourcesRefreshTimeout = 2 * time.Minute
)

// startRefreshLocked launches one background refresh unless one is
// already running. Caller holds c.mu.
func (d *dockerClient) startRefreshLocked() chan struct{} {
	c := &d.resources
	if c.inflight != nil {
		return c.inflight
	}
	done := make(chan struct{})
	c.inflight = done
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), hostResourcesRefreshTimeout)
		defer cancel()
		started := time.Now()
		r, err := d.HostResources(ctx)
		c.mu.Lock()
		if err == nil {
			c.result = &r
			c.at = time.Now()
		} else {
			log.Printf("host resources refresh failed after %s: %v", time.Since(started).Round(time.Millisecond), err)
		}
		c.lastErr = err
		c.inflight = nil
		close(done)
		c.mu.Unlock()
	}()
	return done
}

// WarmHostResources starts the first read at agent start-up, so the first
// dashboard view is usually served from cache too.
func (d *dockerClient) WarmHostResources() {
	d.resources.mu.Lock()
	d.startRefreshLocked()
	d.resources.mu.Unlock()
}

// CachedHostResources returns the latest result, refreshing it in the
// background when stale. Only when nothing has ever been read does it wait
// (bounded by ctx) for the live read to finish.
func (d *dockerClient) CachedHostResources(ctx context.Context) (HostResourcesResult, error) {
	c := &d.resources
	c.mu.Lock()
	if c.result != nil {
		if time.Since(c.at) > hostResourcesMaxAge {
			d.startRefreshLocked()
		}
		r := *c.result
		c.mu.Unlock()
		return r, nil
	}
	done := d.startRefreshLocked()
	c.mu.Unlock()

	select {
	case <-done:
	case <-ctx.Done():
		return HostResourcesResult{}, ctx.Err()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.result == nil {
		return HostResourcesResult{}, c.lastErr
	}
	return *c.result, nil
}
