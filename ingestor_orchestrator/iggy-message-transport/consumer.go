// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package iggytransport

import (
	"context"
	"time"
)

type Message struct {
	Payload   []byte
	Offset    uint64
	Partition uint32
}
type ConsumerDriver interface {
	Join(context.Context, string, string) error
	Leave(context.Context, string, string) error
	Poll(context.Context, string, string, int) ([]Message, error)
	Commit(context.Context, string, string, uint32, uint64) error
}
type Consumer struct {
	Driver       ConsumerDriver
	PollInterval time.Duration
}

func (c Consumer) wait(ctx context.Context) bool {
	interval := c.PollInterval
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (c Consumer) Consume(ctx context.Context, topic, group string, handler func([]byte) error) error {
	joined := false
	for ctx.Err() == nil {
		if err := c.Driver.Join(ctx, topic, group); err == nil {
			joined = true
			break
		}
		if !c.wait(ctx) {
			return nil
		}
	}
	if !joined {
		return nil
	}
	defer func() {
		leaveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		c.Driver.Leave(leaveCtx, topic, group)
	}()
	for ctx.Err() == nil {
		messages, err := c.Driver.Poll(ctx, topic, group, 1)
		if err != nil {
			return err
		}
		for _, m := range messages {
			if err := handler(m.Payload); err != nil {
				return err
			}
			if err := c.Driver.Commit(ctx, topic, group, m.Partition, m.Offset); err != nil {
				return err
			}
		}
		if len(messages) == 0 {
			c.wait(ctx)
		}
	}
	return nil
}
