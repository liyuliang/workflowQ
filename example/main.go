package main

import (
	"context"
	"github.com/liyuliang/workflowQ"
	"time"
)

type QueueExec struct {
}

func (fn QueueExec) Run(ctx context.Context, flowName string) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:

		println("running ", flowName)
		time.Sleep(time.Second)

		println("done ", flowName)
		return flowName + "_id", nil
	}
}

func (fn QueueExec) Result(ctx context.Context, key, result string, timeout time.Duration) error {
	println(key, ":", result)
	return nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	exec := QueueExec{}

	q := workflowQ.NewQueue(3, 10)
	q.SetOptions(workflowQ.SetConcurrency(2))
	q.SetOptions(workflowQ.SetEmptyQueueWaitFn(func() {
		time.Sleep(time.Second * 1)
	}))

	go q.Run(ctx, exec)

	time.Sleep(time.Second * 2)
	go func() {
		if err := q.Push("a:Flow"); err != nil {
			println(err.Error())
		}
	}()
	go func() {
		if err := q.Push("b:Flow"); err != nil {
			println(err.Error())
		}
	}()

	go func() {
		if err := q.Push("c:Flow"); err != nil {
			println(err.Error())
		}
	}()

	go func() {
		time.Sleep(3 * time.Second)
		q.Close()
	}()
	go func() {
		time.Sleep(3 * time.Second)
		cancel()
	}()
	time.Sleep(10 * time.Second)
	println()
	println()
	println("time end")
	println()
	println()
	for err := range q.Errors() {
		println(err.Error())
	}

	println(q.ExecResult("a:Flow"))
	println(q.ExecResult("b:Flow"))
	println(q.ExecResult("a:Flow"))
	println(q.ExecResult("c:Flow"))

}
