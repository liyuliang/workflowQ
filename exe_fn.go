package workflowQ

import (
	"context"
	"time"
)

type ExecFn interface {
	Run(ctx context.Context, key string) (execId string, err error)
	Result(ctx context.Context, key, result string, timeout time.Duration, frequency time.Duration) ([]byte, error)
}

var defaultSleepFn = func() {
	time.Sleep(time.Second)
}

type EmptyQueueFn func()

type RemoveCallbackFn func(key string) error

