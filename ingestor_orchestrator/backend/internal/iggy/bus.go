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

package iggy

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log/slog"
	"time"

	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/backend/internal/app"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/backend/internal/config"
)

type Message struct {
	Payload   []byte
	Offset    uint64
	Partition uint32
}
type Driver interface {
	EnsureStream(context.Context, string) error
	EnsureTopic(context.Context, string, int) error
	Send(context.Context, string, int, []byte) error
	Join(context.Context, string, string) error
	Leave(context.Context, string, string) error
	Poll(context.Context, string, string, int) ([]Message, error)
	Commit(context.Context, string, string, uint32, uint64) error
	Ping(context.Context) error
	Close() error
}

type Bus struct {
	cfg    config.Config
	driver Driver
}

func NewBus(cfg config.Config, driver Driver) *Bus { return &Bus{cfg: cfg, driver: driver} }
func (b *Bus) EnsureTopology(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := b.driver.EnsureStream(ctx, b.cfg.Stream); err != nil {
		return err
	}
	for _, topic := range []struct {
		name       string
		partitions int
	}{
		{b.cfg.JobTopic, b.cfg.JobPartitions}, {b.cfg.RetryTopic, b.cfg.RetryPartitions},
		{b.cfg.ResultTopic, b.cfg.ResultPartitions}, {b.cfg.MetadataTopic, 1}, {b.cfg.MetadataResultTopic, 1},
	} {
		if err := b.driver.EnsureTopic(ctx, topic.name, topic.partitions); err != nil {
			return err
		}
	}
	return nil
}
func (b *Bus) Publish(ctx context.Context, topic string, payload []byte, key string) (app.Routing, error) {
	if err := ctx.Err(); err != nil {
		return app.Routing{}, err
	}
	partitions, err := b.partitionCount(topic)
	if err != nil {
		return app.Routing{}, err
	}
	partition := 0
	if partitions > 1 {
		digest := sha256.Sum256([]byte(key))
		partition = int(binary.BigEndian.Uint32(digest[:4]) % uint32(partitions))
	}
	if err := b.driver.Send(ctx, topic, partition, payload); err != nil {
		return app.Routing{}, err
	}
	return app.Routing{BrokerType: "iggy", Stream: b.cfg.Stream, Topic: topic, Partition: partition}, nil
}
func (b *Bus) partitionCount(topic string) (int, error) {
	switch topic {
	case b.cfg.JobTopic:
		return b.cfg.JobPartitions, nil
	case b.cfg.RetryTopic:
		return b.cfg.RetryPartitions, nil
	case b.cfg.ResultTopic:
		return b.cfg.ResultPartitions, nil
	case b.cfg.MetadataTopic, b.cfg.MetadataResultTopic:
		return 1, nil
	default:
		return 0, fmt.Errorf("unknown Iggy topic %q", topic)
	}
}
func (b *Bus) Consume(ctx context.Context, topic, group string, handler func([]byte) error) error {
	if err := b.driver.Join(ctx, topic, group); err != nil {
		return err
	}
	defer func() {
		leaveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := b.driver.Leave(leaveCtx, topic, group); err != nil {
			slog.Warn("failed to leave Iggy consumer group", "topic", topic, "group", group, "error", err)
		}
	}()
	for {
		if ctx.Err() != nil {
			return nil
		}
		messages, err := b.driver.Poll(ctx, topic, group, 10)
		if err != nil {
			return err
		}
		if len(messages) == 0 {
			timer := time.NewTimer(b.cfg.PollInterval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil
			case <-timer.C:
			}
			continue
		}
		for _, message := range messages {
			if err := handler(message.Payload); err != nil {
				return err
			}
			if err := b.driver.Commit(ctx, topic, group, message.Partition, message.Offset); err != nil {
				return err
			}
		}
	}
}
func (b *Bus) Ping(ctx context.Context) error { return b.driver.Ping(ctx) }
func (b *Bus) Close() error                   { return b.driver.Close() }
