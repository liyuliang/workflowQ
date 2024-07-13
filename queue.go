package workflowQ

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

const (
	StatusInit     = "INIT"
	StatusRunning  = "RUNNING"
	StatusError    = "ERROR"
	StatusFinished = "FINISHED"
)

func NewQueue(cap int, errCap int) *Queue {
	if cap < 10 {
		cap = 10
	}

	if errCap < 1 {
		errCap = 1
	}
	return &Queue{
		concur: 1,
		status: &sync.Map{},
		m:      &sync.Map{},
		q:      make(chan string, cap),
		errCh:  make(chan error, errCap),
		once:   sync.Once{},
		close:  make(chan struct{}),
	}
}

type Queue struct {
	concur int
	status *sync.Map
	m      *sync.Map
	q      chan string
	errCh  chan error
	once   sync.Once
	close  chan struct{}

	fn           ExecFn
	emptyQueueFn EmptyQueueFn
}

func (q *Queue) Push(key string) error {
	if q.q == nil {
		return errors.New("queue is closed")
	}
	if len(q.q) == cap(q.q) {
		return errors.New("queue is fulled")
	}

	if v, ok := q.status.LoadOrStore(key, StatusInit); ok {
		if status, _ := v.(string); status == StatusInit || status == StatusRunning {
			return errors.New("key is existed: " + key)
		} else if status == StatusError || status == StatusFinished {
			q.m.Delete(key)
		}
	}
	q.q <- key
	return nil
}

func (q *Queue) Run(ctx context.Context) {
	var exit bool
	defer q.closeCh()

	for {

		select {
		case <-q.close:
			fmt.Printf("%v \n", "主动退出")
			exit = true
			return
		case <-ctx.Done():
			fmt.Printf("%v \n", "超时退出")
			exit = true
			return

		default:

			if len(q.q) == 0 {
				q.emptyQueueFn()
				continue
			}

			if exit {
				return
			}

			var c = q.concur
			if len(q.q) < c {
				c = len(q.q)
			}

			wg := sync.WaitGroup{}
			wg.Add(c)

			for i := 0; i < c; i++ {

				go func() {
					defer wg.Done()

					if err := q.exec(ctx); err != nil {
						q.pushErrCh(err)
					}
				}()
			}
			wg.Wait()
		}
	}
}

func (q *Queue) exec(ctx context.Context) error {
	if len(q.q) == 0 {
		return errors.New("queue is empty")
	}

	k, ok := <-q.q
	if !ok {
		return errors.New("queue is closed")
	}
	if k == "" {
		return errors.New("queue is empty")
	}

	q.status.Store(k, StatusRunning)

	result, err := q.fn(ctx, k)
	if err != nil {
		q.status.Store(k, StatusError)
		return err
	}

	q.status.Store(k, StatusFinished)
	q.m.Store(k, result)
	return nil
}

func (q *Queue) pushErrCh(err error) {
	if q.errCh == nil {
		return
	}
	if len(q.errCh) == cap(q.errCh) {
		<-q.errCh
	}
	q.errCh <- err
}

func (q *Queue) Errors() <-chan error {
	return q.errCh
}

func (q *Queue) ExecResult(k string) string {
	if q.m == nil {
		return ""
	}
	val, _ := q.m.Load(k)
	if v, ok := val.(string); ok {
		return v
	}
	return ""
}

func (q *Queue) closeCh() {
	q.once.Do(func() {
		close(q.q)
		close(q.errCh)
	})
}

// Close You can either proactively initiate a close or control the exit through the context.
func (q *Queue) Close() {
	q.close <- struct{}{}
}

type ExecFn func(ctx context.Context, key string) (string, error)

type EmptyQueueFn func()

func SetConcurrency(c int) QueueOption {
	if c < 1 {
		c = 1
	}
	return func(q *Queue) {
		q.concur = c
	}
}

func SetWaitFn(fn EmptyQueueFn) QueueOption {
	return func(q *Queue) {
		q.emptyQueueFn = fn
	}
}

type QueueOption func(*Queue)

func (q *Queue) SetOptions(opts ...QueueOption) {
	for _, opt := range opts {
		opt(q)
	}
}

func (q *Queue) SetExecFn(fn ExecFn, force bool) {
	if force || q.fn == nil {
		q.fn = fn
	}
}

func (q *Queue) IsRunning(k string) bool {
	val, _ := q.status.Load(k)
	if status, ok := val.(string); ok && (status == StatusInit || status == StatusRunning) {
		return true
	}
	return false
}
