package workflowQ

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
