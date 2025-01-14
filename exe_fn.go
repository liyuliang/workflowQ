package workflowQ

import (
	"context"
	"time"
)

type ExecFn interface {
	Run(ctx context.Context, key string, data map[string]interface{}) (string, error)
	Result(ctx context.Context, key, result string, timeOpts TimeOptions) (string, error)
}

var defaultSleepFn = func() {
	time.Sleep(time.Second)
}

type EmptyQueueFn func()

type RemoveCallbackFn func(key string) error
