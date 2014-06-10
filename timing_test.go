package enproxy

import (
	"testing"
	"time"
)

func TestTimingHistory(t *testing.T) {
	th := newTimingHistory(5 * time.Second)
	if th.estimate() != 5*time.Second {
		t.Errorf("Wrong estimate after default.  Expected %s, got %s",
			5*time.Second,
			th.estimate())
	}

	th.record(15 * time.Second)
	if th.min != 5*time.Second {
		t.Errorf("Wrong min after recording observation.  Expected %s, got %s",
			5*time.Second,
			th.min)
	}
	if th.max != 15*time.Second {
		t.Errorf("Wrong max after recording observation.  Expected %s, got %s",
			15*time.Second,
			th.max)
	}
	if th.count != 2 {
		t.Errorf("Wrong count after recording observation.  Expected %s, got %s",
			15*time.Second,
			th.count)
	}
	if th.total != 20*time.Second {
		t.Errorf("Wrong total after recording observation.  Expected %s, got %s",
			20*time.Second,
			th.count)
	}
	if th.estimate() != 10*time.Second {
		t.Errorf("Wrong estimate after recording observation.  Expected %s, got %s",
			10*time.Second,
			th.estimate())
	}
}

func TestReadTimings(t *testing.T) {
	p := newReadTimings(10*time.Second, 50*time.Millisecond)
	if p.first.estimate() != 10*time.Second {
		t.Errorf("Wrong estimate for first.  Expected %s, got %s",
			10*time.Second,
			p.first.estimate())
	}
	if p.subsequent.estimate() != 50*time.Millisecond {
		t.Errorf("Wrong estimate for subsequent.  Expected %s, got %s",
			50*time.Millisecond,
			p.subsequent.estimate())
	}
}
