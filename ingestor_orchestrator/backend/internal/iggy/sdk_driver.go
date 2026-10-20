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

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/backend/internal/config"
	iggytransport "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/iggy-message-transport"
)

type SDKDriver struct {
	client    topicClient
	stream    string
	transport *iggytransport.Client
}

type topicClient interface {
	GetStream(context.Context, iggcon.Identifier) (*iggcon.StreamDetails, error)
	CreateStream(context.Context, string) (*iggcon.StreamDetails, error)
	GetTopic(context.Context, iggcon.Identifier, iggcon.Identifier) (*iggcon.TopicDetails, error)
	CreateTopic(context.Context, iggcon.Identifier, string, uint32, iggcon.CompressionAlgorithm, iggcon.Duration, uint64, ...iggcon.HeaderEntry) (*iggcon.TopicDetails, error)
	UpdateTopic(context.Context, iggcon.Identifier, iggcon.Identifier, string, iggcon.CompressionAlgorithm, iggcon.Duration, uint64, ...iggcon.HeaderEntry) error
	Ping(context.Context) error
}

const maxTopicSize = uint64(2 * 1024 * 1024 * 1024)

const messageExpiry = iggcon.Duration(30 * 24 * iggcon.Hour)

func NewSDKDriver(ctx context.Context, cfg config.Config) (*SDKDriver, error) {
	transport, err := iggytransport.Connect(ctx, iggytransport.ConnectionConfig{Address: cfg.IggyAddress, Username: cfg.IggyUsername, Password: cfg.IggyPassword, Stream: cfg.Stream})
	if err != nil {
		return nil, err
	}
	return &SDKDriver{client: transport.RawClient(), stream: cfg.Stream, transport: transport}, nil
}

func identifier(value string) (iggcon.Identifier, error) { return iggcon.NewIdentifier(value) }

func (d *SDKDriver) EnsureStream(ctx context.Context, name string) error {
	id, err := identifier(name)
	if err != nil {
		return err
	}
	if current, err := d.client.GetStream(ctx, id); err == nil {
		// Workers own creation and partition counts for retry and parquet topics.
		// Reconcile retention for every existing topic in the application stream.
		for _, topic := range current.Topics {
			if topic.MessageExpiry == messageExpiry && topic.MaxTopicSize == maxTopicSize {
				continue
			}
			topicID, err := identifier(topic.Name)
			if err != nil {
				return err
			}
			if err := d.client.UpdateTopic(ctx, id, topicID, topic.Name, iggcon.CompressionAlgorithm(topic.CompressionAlgorithm), messageExpiry, maxTopicSize); err != nil {
				return err
			}
		}
		return nil
	}
	if _, err := d.client.CreateStream(ctx, name); err != nil {
		if _, getErr := d.client.GetStream(ctx, id); getErr != nil {
			return err
		}
	}
	return nil
}

func (d *SDKDriver) EnsureTopic(ctx context.Context, name string, partitions int) error {
	stream, err := identifier(d.stream)
	if err != nil {
		return err
	}
	topic, err := identifier(name)
	if err != nil {
		return err
	}
	if current, err := d.client.GetTopic(ctx, stream, topic); err == nil {
		if current.MessageExpiry == messageExpiry && current.MaxTopicSize == maxTopicSize {
			return nil
		}
		return d.client.UpdateTopic(ctx, stream, topic, name, iggcon.CompressionAlgorithm(current.CompressionAlgorithm), messageExpiry, maxTopicSize)
	}
	if _, err := d.client.CreateTopic(ctx, stream, name, uint32(partitions), 0, messageExpiry, maxTopicSize); err != nil {
		if _, getErr := d.client.GetTopic(ctx, stream, topic); getErr != nil {
			return err
		}
	}
	return nil
}

func (d *SDKDriver) Send(ctx context.Context, topicName, key string, payload []byte) (int, error) {
	return d.transport.Send(ctx, topicName, key, payload)
}

func (d *SDKDriver) Join(ctx context.Context, topic, group string) error {
	return d.transport.Join(ctx, topic, group)
}
func (d *SDKDriver) Leave(ctx context.Context, topic, group string) error {
	return d.transport.Leave(ctx, topic, group)
}
func (d *SDKDriver) Poll(ctx context.Context, topic, group string, count int) ([]Message, error) {
	values, err := d.transport.Poll(ctx, topic, group, count)
	if err != nil {
		return nil, err
	}
	messages := make([]Message, len(values))
	for i, m := range values {
		messages[i] = Message{Payload: m.Payload, Offset: m.Offset, Partition: m.Partition}
	}
	return messages, nil
}
func (d *SDKDriver) Commit(ctx context.Context, topic, group string, partition uint32, offset uint64) error {
	return d.transport.Commit(ctx, topic, group, partition, offset)
}

func (d *SDKDriver) Ping(ctx context.Context) error { return d.client.Ping(ctx) }
func (d *SDKDriver) Close() error                   { return d.transport.Close() }

var _ Driver = (*SDKDriver)(nil)
