package enproxy

import (
	"fmt"
	"sync"
	"time"
)

// timingHistory records timing history
type timingHistory struct {
	count int64
	total time.Duration
	min   time.Duration
	max   time.Duration
	mutex sync.RWMutex
}

func newTimingHistory(dflt time.Duration) *timingHistory {
	return &timingHistory{
		count: 1,
		total: dflt,
		min:   dflt,
		max:   dflt,
	}
}

// record records an observed timing to update this timing information
func (th *timingHistory) record(observed time.Duration) {
	th.mutex.Lock()
	defer th.mutex.Unlock()
	th.count += 1
	th.total += observed
	if observed < th.min {
		th.min = observed
	}
	if observed > th.max {
		th.max = observed
	}
}

// estimate() provides an estimated timing based on the history.  Right now,
// this is just the average timing.
func (th *timingHistory) estimate() time.Duration {
	th.mutex.RLock()
	defer th.mutex.RUnlock()
	average := th.total.Nanoseconds() / th.count
	return time.Duration(average) * time.Nanosecond
}

// readTimings records a profile of read timings for a particular site
type readTimings struct {
	first      *timingHistory // history for first read
	subsequent *timingHistory // hostory for second and subsequent reads
}

func newReadTimings(
	defaultFirst time.Duration,
	defaultSubsequent time.Duration) *readTimings {
	return &readTimings{
		first:      newTimingHistory(defaultFirst),
		subsequent: newTimingHistory(defaultSubsequent),
	}
}

func (rt *readTimings) String() string {
	return fmt.Sprintf("first: %s    subsequent: %s", rt.first.estimate(), rt.subsequent.estimate())
}
