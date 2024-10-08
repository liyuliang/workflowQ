package workflowQ

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	StatusInit        = "INIT"
	StatusRunning     = "RUNNING"
	StatusError       = "ERROR"
	StatusErrorResult = "ERROR_RESULT"
	StatusFinished    = "FINISHED"
	StatusDeleted     = "DELETED"
)

func NewQueue(cap int, errCap int) *Queue {
	if cap < 10 {
		cap = 10
	}

	if errCap < 1 {
		errCap = 1
	}
	return &Queue{
		concur:            1,
		status:            &sync.Map{},
		m:                 &sync.Map{},
		q:                 make(chan string, cap),
		errCh:             make(chan error, errCap),
		once:              sync.Once{},
		close:             make(chan struct{}),
		waitResultTimeout: time.Second * 10,
	}
}

type Queue struct {
	concur            int
	status            *sync.Map
	m                 *sync.Map
	q                 chan string
	errCh             chan error
	once              sync.Once
	close             chan struct{}
	waitResultTimeout time.Duration
	emptyQueueFn      EmptyQueueFn
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
		} else /*if status == StatusError || status == StatusFinished || status== StatusDeleted*/ {
			q.status.Store(key, StatusInit)
			q.m.Delete(key)
		}
	}
	q.q <- key
	return nil
}

func (q *Queue) Run(ctx context.Context, fn ExecFn) {
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
				q.runEmptyQueueFn()
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

					if err := q.exec(ctx, fn); err != nil {
						q.pushErrCh(err)
					}
				}()
			}
			wg.Wait()
		}
	}
}

func (q *Queue) exec(ctx context.Context, fn ExecFn) error {
	if fn == nil {
		return errors.New("fn is nil")
	}
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

	if v, ok := q.status.LoadOrStore(k, StatusRunning); ok {
		if status, _ := v.(string); status == StatusDeleted {
			return errors.New("task is deleted:" + k)
		}
	}

	q.status.Store(k, StatusRunning)

	result, err := fn.Run(ctx, k)
	if err != nil {
		q.status.Store(k, StatusError)
		return err
	}
	q.m.Store(k, result)

	if fn.Result != nil {
		err = fn.Result(ctx, k, result, q.waitResultTimeout)
		if err != nil {
			q.status.Store(k, StatusErrorResult)
			return err
		}
	}
	q.status.Store(k, StatusFinished)
	return nil
}

func (q *Queue) pushErrCh(err error) {
	if q.errCh == nil {
		return
	}
	for len(q.errCh) == cap(q.errCh) {
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

func (q *Queue) IsRunning(k string) bool {
	val, _ := q.status.Load(k)
	if status, ok := val.(string); ok && (status == StatusInit || status == StatusRunning) {
		return true
	}
	return false
}

func (q *Queue) Remove(k string, callback RemoveCallbackFn) error {
	// 设置状态为删除
	q.status.Store(k, StatusDeleted)
	return callback(k)
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

func SetConcurrency(c int) QueueOption {
	if c < 1 {
		c = 1
	}
	return func(q *Queue) {
		q.concur = c
	}
}

func SetEmptyQueueWaitFn(fn EmptyQueueFn) QueueOption {
	return func(q *Queue) {
		q.emptyQueueFn = fn
	}
}

func SetWaitResultTimeout(timeout time.Duration) QueueOption {
	return func(q *Queue) {
		q.waitResultTimeout = timeout
	}
}
