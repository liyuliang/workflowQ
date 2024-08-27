package workflowQ

import (
	"context"
	"time"
)

type ExecFn interface {
	Run(ctx context.Context, key string) (string, error)
	Result(ctx context.Context, result string, timeout time.Duration) error
}

var defaultSleepFn = func() {
	time.Sleep(time.Second)
}

type EmptyQueueFn func()

type RemoveCallbackFn func(key string) error
