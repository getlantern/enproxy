package enproxy

import (
	"math"
	"time"
)

const (
	INFINITY = math.MaxInt64
)

type TimeoutProfile struct {
	first *timeoutPeriod
}

func NewTimeoutProfile(initialTimeout time.Duration) *TimeoutProfile {
	first := &timeoutPeriod{INFINITY, initialTimeout, nil}
	return &TimeoutProfile{first}
}

func (t *TimeoutProfile) WithTimeoutAfter(bytes int, timeout time.Duration) *TimeoutProfile {
	nextPeriod := &timeoutPeriod{INFINITY, timeout, nil}
	priorPeriod := t.getPeriodAfter(bytes - 1)
	if priorPeriod.next != nil {
		// We're inserting ourselves
		nextPeriod.bytes = priorPeriod.bytes
		nextPeriod.next = priorPeriod.next
	}
	priorPeriod.bytes = bytes
	priorPeriod.next = nextPeriod
	return t
}

func (t *TimeoutProfile) GetTimeoutAfter(bytes int) time.Duration {
	return t.getPeriodAfter(bytes).timeout
}

func (t *TimeoutProfile) getPeriodAfter(bytes int) *timeoutPeriod {
	tp := t.first
	for {
		if tp.bytes > bytes || tp.next == nil {
			break
		}
		tp = tp.next
	}
	return tp
}

type timeoutPeriod struct {
	bytes   int
	timeout time.Duration
	next    *timeoutPeriod
}
