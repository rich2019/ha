package fence

import "context"

type Manager interface {
	Fence(context.Context, string) error
}

type NoopManager struct{}

func (NoopManager) Fence(context.Context, string) error { return nil }
