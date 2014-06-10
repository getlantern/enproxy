package enproxy

import (
	"sync"
	"time"
)

// latencyHistory records latency history
type latencyHistory struct {
	count int64
	total time.Duration
	min   time.Duration
	max   time.Duration
	mutex sync.RWMutex
}

func newLatencyHistory(dflt time.Duration) *latencyHistory {
	return &latencyHistory{
		count: 1,
		total: dflt,
		min:   dflt,
		max:   dflt,
	}
}

// record records an observed latency to update this latency information
func (lh *latencyHistory) record(observed time.Duration) {
	lh.mutex.Lock()
	defer lh.mutex.Unlock()
	lh.count += 1
	lh.total += observed
	if observed < lh.min {
		lh.min = observed
	}
	if observed > lh.max {
		lh.max = observed
	}
}

// estimate() provides an estimated latency based on the history.  Right now,
// this is just the average latency.
func (lh *latencyHistory) estimate() time.Duration {
	lh.mutex.RLock()
	defer lh.mutex.RUnlock()
	average := lh.total.Nanoseconds() / lh.count
	return time.Duration(average) * time.Nanosecond
}

// latencyProfile records a profile of latency histories for a particular site
type latencyProfile struct {
	firstRead  *latencyHistory // history for first read
	secondRead *latencyHistory // hostory for second and subsequent reads
}

func newLatencyProfile(
	defaultFirstRead time.Duration,
	defaultSecondRead time.Duration) *latencyProfile {
	return &latencyProfile{
		firstRead:  newLatencyHistory(defaultFirstRead),
		secondRead: newLatencyHistory(defaultSecondRead),
	}
}
