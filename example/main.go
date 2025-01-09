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

func (fn QueueExec) Result(ctx context.Context, key, result string, timeout time.Duration, frequency time.Duration) error {

	//	timeout, frequency := timeOpts()
	//
	//	checker := time.NewTicker(frequency)
	//	defer checker.Stop()
	//
	//	ender := time.NewTimer(timeout)
	//	defer ender.Stop()
	//
	//	// 等执行完
	//WaitLoop:
	//	for {
	//		select {
	//		case <-ctx.Done():
	//			break WaitLoop
	//
	//		case <-ender.C:
	//			fmt.Printf("exec timeout\n")
	//			break WaitLoop
	//
	//		case <-checker.C:
	//			// 每5秒检测一次结果
	//			fmt.Printf("exec check...\n")
	//		}
	//	}
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
	q.SetOptions(workflowQ.SetDefaultTimeout(time.Second * 5))
	q.SetOptions(workflowQ.SetDefaultCheckFrequency(time.Second * 2))

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
	//
	//go func() {
	//	time.Sleep(3 * time.Second)
	//	q.Close()
	//}()
	go func() {
		time.Sleep(20 * time.Second)
		cancel()
	}()
	time.Sleep(20 * time.Second)
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

