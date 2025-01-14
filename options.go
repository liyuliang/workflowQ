package workflowQ

import "time"

type QueueOption func(*Queue)

func (q *Queue) SetOptions(opts ...QueueOption) {
	for _, opt := range opts {
		opt(q)
	}
}

func (q *Queue) runEmptyQueueFn() {
	if q.emptyQueueFn == nil {
		q.emptyQueueFn = defaultSleepFn
	}
	q.emptyQueueFn()
}

type TimeOptions func() (timeoutSec time.Duration, frequencySec time.Duration)
