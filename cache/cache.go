// Package cache decouples Prometheus scrapes from OpenStack API sweeps.
//
// Without the cache every scrape of /metrics runs a full sweep of every
// enabled OpenStack service, so N scrapers mean N concurrent sweeps. With the
// cache a single background loop runs the sweep and stores the result, and
// every scrape is served from that stored result.
//
// Behaviour (ported from upstream openstack-exporter's --cache):
//   - a sweep runs at start-up and then every interval;
//   - sweeps never overlap: if a sweep takes longer than interval the next one
//     starts as soon as it finishes;
//   - the stored result is replaced only once a sweep has fully completed, so
//     a scrape never sees a mix of old and new values;
//   - a stored result older than ttl is treated as expired and scrapes get no
//     metrics until the next sweep completes.
package cache

import (
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Gatherer is a prometheus.Gatherer that serves the last result gathered from
// source instead of gathering on every call.
type Gatherer struct {
	source   prometheus.Gatherer
	ttl      time.Duration
	interval time.Duration
	now      func() time.Time
	logger   log.Logger

	mu       sync.RWMutex
	families []*dto.MetricFamily
	updated  time.Time
}

// New returns a Gatherer that sweeps source every interval and serves each
// sweep's result for up to ttl.
func New(source prometheus.Gatherer, ttl, interval time.Duration, logger log.Logger) *Gatherer {
	return &Gatherer{source: source, ttl: ttl, interval: interval, now: time.Now, logger: logger}
}

// Refresh runs one sweep against source and stores the result.
//
// prometheus.Gatherer may return partial results together with an error; the
// partial results are stored, matching what an uncached scrape would serve.
func (c *Gatherer) Refresh() error {
	families, err := c.source.Gather()
	if families != nil {
		c.mu.Lock()
		c.families = families
		c.updated = c.now()
		c.mu.Unlock()
	}
	return err
}

// Gather implements prometheus.Gatherer. It never contacts OpenStack.
func (c *Gatherer) Gather() ([]*dto.MetricFamily, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.updated.IsZero() || c.now().Sub(c.updated) > c.ttl {
		return nil, nil
	}
	return c.families, nil
}

// Run refreshes the cache immediately and then every interval until stop is
// closed. It blocks, so call it in its own goroutine.
func (c *Gatherer) Run(stop <-chan struct{}) {
	level.Info(c.logger).Log("msg", "Cache: starting background collection", "ttl", c.ttl, "interval", c.interval)
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		c.refreshAndLog()
		select {
		case <-ticker.C:
		case <-stop:
			level.Info(c.logger).Log("msg", "Cache: background collection stopped")
			return
		}
	}
}

func (c *Gatherer) refreshAndLog() {
	start := c.now()
	if err := c.Refresh(); err != nil {
		level.Error(c.logger).Log("msg", "Cache: collection finished with errors", "err", err)
	}
	level.Info(c.logger).Log("msg", "Cache: collection finished", "duration", c.now().Sub(start))
}
