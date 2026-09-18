// Noctcloud Desenvolvimento LTDA
// Copyright (C) 2026  Noctcloud Desenvolvimento LTDA
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/backend/internal/app"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/backend/internal/config"
)

type Consumer interface {
	Consume(context.Context, string, string, func([]byte) error) error
}
type ResultProcessor interface {
	ApplyJobResult(context.Context, app.JobResultMessage) error
	ApplyMetadataResult(context.Context, app.MetadataSyncResultMessage) error
}
type Consumers struct {
	bus       Consumer
	processor ResultProcessor
	cfg       config.Config
	active    atomic.Int32
}

func NewConsumers(bus Consumer, processor ResultProcessor, cfg config.Config) *Consumers {
	return &Consumers{bus: bus, processor: processor, cfg: cfg}
}
func (c *Consumers) Ready() bool { return c.active.Load() == 2 }
func (c *Consumers) Start(ctx context.Context) {
	go c.run(ctx, c.cfg.ResultTopic, c.cfg.ResultGroup, c.handleJob)
	go c.run(ctx, c.cfg.MetadataResultTopic, c.cfg.MetadataResultGroup, c.handleMetadata)
}
func (c *Consumers) run(ctx context.Context, topic, group string, handler func(context.Context, []byte) error) {
	delay := time.Second
	for ctx.Err() == nil {
		c.active.Add(1)
		err := c.bus.Consume(ctx, topic, group, func(payload []byte) error { return handler(ctx, payload) })
		c.active.Add(-1)
		if ctx.Err() != nil {
			return
		}
		slog.Error("Iggy consumer disconnected", "topic", topic, "error", err, "retry_in", delay)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if delay < 30*time.Second {
			delay *= 2
			if delay > 30*time.Second {
				delay = 30 * time.Second
			}
		}
	}
}
func (c *Consumers) handleJob(ctx context.Context, payload []byte) error {
	var message app.JobResultMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		return err
	}
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		err = c.processor.ApplyJobResult(ctx, message)
		if !errors.Is(err, app.ErrNotFound) {
			return err
		}
		if attempt < 4 {
			timer := time.NewTimer(time.Duration(1<<attempt) * 100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	slog.Error("job result references unknown job", "job_id", message.JobID)
	return nil
}
func (c *Consumers) handleMetadata(ctx context.Context, payload []byte) error {
	var message app.MetadataSyncResultMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		return err
	}
	return c.processor.ApplyMetadataResult(ctx, message)
}
