package route

import "context"

type Manager interface {
	SetPrimary(context.Context, string) error
}

type NoopManager struct{}

func (NoopManager) SetPrimary(context.Context, string) error { return nil }
