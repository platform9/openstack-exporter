package cache

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
)

// countingGatherer records how many sweeps were run against it.
type countingGatherer struct {
	calls    int32
	families []*dto.MetricFamily
	err      error
}

func (g *countingGatherer) Gather() ([]*dto.MetricFamily, error) {
	atomic.AddInt32(&g.calls, 1)
	return g.families, g.err
}

func family(name string) *dto.MetricFamily {
	return &dto.MetricFamily{Name: &name}
}

func TestGatherBeforeFirstRefreshIsEmpty(t *testing.T) {
	src := &countingGatherer{families: []*dto.MetricFamily{family("a")}}
	c := New(src, time.Minute, log.NewNopLogger())

	mfs, err := c.Gather()
	assert.NoError(t, err)
	assert.Empty(t, mfs)
	assert.Equal(t, int32(0), src.calls, "Gather must not sweep")
}

func TestManyScrapesShareOneSweep(t *testing.T) {
	src := &countingGatherer{families: []*dto.MetricFamily{family("a"), family("b")}}
	c := New(src, time.Minute, log.NewNopLogger())

	assert.NoError(t, c.Refresh())
	for i := 0; i < 10; i++ {
		mfs, err := c.Gather()
		assert.NoError(t, err)
		assert.Equal(t, src.families, mfs)
	}
	assert.Equal(t, int32(1), src.calls)
}

func TestExpiredCacheIsEmpty(t *testing.T) {
	src := &countingGatherer{families: []*dto.MetricFamily{family("a")}}
	c := New(src, time.Minute, log.NewNopLogger())
	now := time.Unix(1000, 0)
	c.now = func() time.Time { return now }

	assert.NoError(t, c.Refresh())

	now = now.Add(time.Minute)
	mfs, _ := c.Gather()
	assert.Len(t, mfs, 1, "exactly ttl old is still valid")

	now = now.Add(time.Second)
	mfs, _ = c.Gather()
	assert.Empty(t, mfs, "older than ttl is expired")
}

func TestPartialResultsAreStored(t *testing.T) {
	src := &countingGatherer{families: []*dto.MetricFamily{family("a")}, err: errors.New("boom")}
	c := New(src, time.Minute, log.NewNopLogger())

	assert.Error(t, c.Refresh())
	mfs, _ := c.Gather()
	assert.Len(t, mfs, 1)
}

func TestFailedRefreshKeepsPreviousResult(t *testing.T) {
	src := &countingGatherer{families: []*dto.MetricFamily{family("a")}}
	c := New(src, time.Minute, log.NewNopLogger())
	assert.NoError(t, c.Refresh())

	src.families, src.err = nil, errors.New("boom")
	assert.Error(t, c.Refresh())

	mfs, _ := c.Gather()
	assert.Len(t, mfs, 1)
}

func TestRunRefreshesUntilStopped(t *testing.T) {
	src := &countingGatherer{families: []*dto.MetricFamily{family("a")}}
	c := New(src, 20*time.Millisecond, log.NewNopLogger())
	stop := make(chan struct{})
	done := make(chan struct{})

	go func() { c.Run(stop); close(done) }()
	time.Sleep(55 * time.Millisecond)
	close(stop)
	<-done

	// One sweep at start-up plus roughly one every 10ms.
	calls := atomic.LoadInt32(&src.calls)
	assert.True(t, calls >= 3, "expected at least 3 sweeps, got %d", calls)
}

func TestServesRealRegistry(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: "openstack_test_gauge", Help: "h"})
	reg.MustRegister(g)
	g.Set(1)

	c := New(reg, time.Minute, log.NewNopLogger())
	assert.NoError(t, c.Refresh())

	g.Set(2) // a scrape between sweeps must still see the swept value
	mfs, err := c.Gather()
	assert.NoError(t, err)
	assert.Len(t, mfs, 1)
	assert.Equal(t, 1.0, mfs[0].Metric[0].GetGauge().GetValue())
}
