package main

import (
	"context"
	"errors"
	"github.com/liyuliang/workflowQ"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	fn := func(ctx context.Context, flowName string) (string, error) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:

			println("running ", flowName)
			time.Sleep(time.Second)
			println("done ", flowName)
			if flowName == "c:Flow" || flowName == "b:Flow" {
				return "", errors.New("flow get error:" + flowName)
			}

			return flowName + "_id", nil
		}
	}

	q := workflowQ.NewQueue(3, 10)
	q.SetOptions(workflowQ.SetConcurrency(2))
	q.SetOptions(workflowQ.SetEmptyQueueWaitFn(func() {
		time.Sleep(time.Second * 1)
	}))
	q.SetExecFn(fn, true)

	go q.Run(ctx)

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
		if err := q.Push("a:Flow"); err != nil {
			q.Remove("a:Flow", func(key string) {
				println(key, "is removed")
			})
			println(err.Error())
		}
	}()
	go func() {
		if err := q.Push("c:Flow"); err != nil {
			println(err.Error())
		}
	}()
	go func() {
		if err := q.Push("b:Flow"); err != nil {
			q.Remove("b:Flow", func(key string) {
				println(key, "is removed")
			})
			println(err.Error())
		}
	}()

	go func() {
		time.Sleep(6 * time.Second)
		q.Close()
	}()
	go func() {
		time.Sleep(8 * time.Second)
		cancel()
	}()
	time.Sleep(10 * time.Second)
	println("time end")
	for err := range q.Errors() {
		println(err.Error())
	}

	println(q.ExecResult("a:Flow"))
	println(q.ExecResult("b:Flow"))
	println(q.ExecResult("a:Flow"))
	println(q.ExecResult("c:Flow"))

}
