package enproxy

import (
	"testing"
	"time"
)

func TestLatencyHistory(t *testing.T) {
	lh := newLatencyHistory(5 * time.Second)
	if lh.estimate() != 5*time.Second {
		t.Errorf("Wrong estimate after default.  Expected %s, got %s",
			5*time.Second,
			lh.estimate())
	}

	lh.record(15 * time.Second)
	if lh.min != 5*time.Second {
		t.Errorf("Wrong min after recording observation.  Expected %s, got %s",
			5*time.Second,
			lh.min)
	}
	if lh.max != 15*time.Second {
		t.Errorf("Wrong max after recording observation.  Expected %s, got %s",
			15*time.Second,
			lh.max)
	}
	if lh.count != 2 {
		t.Errorf("Wrong count after recording observation.  Expected %s, got %s",
			15*time.Second,
			lh.count)
	}
	if lh.total != 20*time.Second {
		t.Errorf("Wrong total after recording observation.  Expected %s, got %s",
			20*time.Second,
			lh.count)
	}
	if lh.estimate() != 10*time.Second {
		t.Errorf("Wrong estimate after recording observation.  Expected %s, got %s",
			10*time.Second,
			lh.estimate())
	}
}

func TestLatencyProfile(t *testing.T) {
	p := newLatencyProfile(10*time.Second, 50*time.Millisecond)
	if p.firstRead.estimate() != 10*time.Second {
		t.Errorf("Wrong estimate for firstRead.  Expected %s, got %s",
			10*time.Second,
			p.firstRead.estimate())
	}
	if p.secondRead.estimate() != 50*time.Millisecond {
		t.Errorf("Wrong estimate for firstRead.  Expected %s, got %s",
			50*time.Millisecond,
			p.secondRead.estimate())
	}
}
