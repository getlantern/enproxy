package enproxy

import (
	"testing"
	"time"
)

func TestTimeoutProfile(t *testing.T) {
	var timeout1 = 50 * time.Millisecond
	var timeout2 = 100 * time.Millisecond
	var timeout3 = 150 * time.Millisecond

	var cutoff2 = 2000
	var cutoff3 = 3000
	var reallyHighCutoff = 1000000000

	p := NewTimeoutProfile(timeout1)
	to := p.GetTimeoutAfter(0)
	if to != timeout1 {
		t.Errorf("Wrong initial timeout for 0 cutoff.  Expected %s, got %s", timeout1, to)
	}

	to = p.GetTimeoutAfter(reallyHighCutoff)
	if to != timeout1 {
		t.Errorf("Wrong initial timeout for really high cutoff.  Expected %s, got %s", timeout1, to)
	}

	// Add timeout
	p.WithTimeoutAfter(cutoff3, timeout3)
	to = p.GetTimeoutAfter(0)
	if to != timeout1 {
		t.Errorf("With 2 periods, wrong timeout for 0 cutoff.  Expected %s, got %s", timeout1, to)
	}

	to = p.GetTimeoutAfter(cutoff3)
	if to != timeout3 {
		t.Errorf("With 2 periods, wrong timeout for high cutoff.  Expected %s, got %s", timeout3, to)
	}

	to = p.GetTimeoutAfter(reallyHighCutoff)
	if to != timeout3 {
		t.Errorf("With 2 periods, wrong timeout for really high cutoff.  Expected %s, got %s", timeout3, to)
	}

	// Insert timeout out of order
	p.WithTimeoutAfter(cutoff2, timeout2)
	to = p.GetTimeoutAfter(0)
	if to != timeout1 {
		t.Errorf("With 3 periods, wrong timeout for 0 cutoff.  Expected %s, got %s", timeout1, to)
	}

	to = p.GetTimeoutAfter(cutoff2)
	if to != timeout2 {
		t.Errorf("With 3 periods, wrong timeout for middle cutoff.  Expected %s, got %s", timeout2, to)
	}

	to = p.GetTimeoutAfter(cutoff3)
	if to != timeout3 {
		t.Errorf("With 3 periods, wrong timeout for high cutoff.  Expected %s, got %s", timeout3, to)
	}

	to = p.GetTimeoutAfter(reallyHighCutoff)
	if to != timeout3 {
		t.Errorf("With 3 periods, wrong timeout for really high cutoff.  Expected %s, got %s", timeout3, to)
	}
}
