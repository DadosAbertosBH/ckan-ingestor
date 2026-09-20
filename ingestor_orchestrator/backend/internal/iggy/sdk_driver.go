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
	"fmt"

	"github.com/apache/iggy/foreign/go/client"
	"github.com/apache/iggy/foreign/go/client/tcp"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/google/uuid"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/backend/internal/config"
)

type SDKDriver struct {
	client iggcon.Client
	stream string
}

func NewSDKDriver(ctx context.Context, cfg config.Config) (*SDKDriver, error) {
	cli, err := client.NewIggyClient(client.WithTcp(
		tcp.WithServerAddress(cfg.IggyAddress),
		tcp.WithAutoLogin(tcp.NewUsernamePasswordCredentials(cfg.IggyUsername, cfg.IggyPassword)),
	))
	if err != nil {
		return nil, err
	}
	if err := cli.Connect(ctx); err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("connect to Iggy: %w", err)
	}
	return &SDKDriver{client: cli, stream: cfg.Stream}, nil
}

func identifier(value string) (iggcon.Identifier, error) { return iggcon.NewIdentifier(value) }

func (d *SDKDriver) EnsureStream(ctx context.Context, name string) error {
	id, err := identifier(name)
	if err != nil {
		return err
	}
	if _, err := d.client.GetStream(ctx, id); err == nil {
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
	if _, err := d.client.GetTopic(ctx, stream, topic); err == nil {
		return nil
	}
	if _, err := d.client.CreateTopic(ctx, stream, name, uint32(partitions), 0, iggcon.IggyExpiryServerDefault, 0); err != nil {
		if _, getErr := d.client.GetTopic(ctx, stream, topic); getErr != nil {
			return err
		}
	}
	return nil
}

func (d *SDKDriver) Send(ctx context.Context, topicName, key string, payload []byte) (int, error) {
	stream, err := identifier(d.stream)
	if err != nil {
		return 0, err
	}
	topic, err := identifier(topicName)
	if err != nil {
		return 0, err
	}
	message, err := iggcon.NewIggyMessage(payload)
	if err != nil {
		return 0, err
	}
	entityID, err := uuid.Parse(key)
	if err != nil {
		return 0, fmt.Errorf("parse Iggy message key as UUID: %w", err)
	}
	response, err := d.client.SendMessages(ctx, stream, topic, iggcon.EntityIdGuid(entityID), []iggcon.IggyMessage{message})
	if err != nil {
		return 0, err
	}
	if len(response.Confirmations) == 0 {
		return 0, fmt.Errorf("Iggy did not confirm message routing")
	}
	return int(response.Confirmations[0].PartitionId), nil
}

func (d *SDKDriver) Join(ctx context.Context, topicName, groupName string) error {
	stream, err := identifier(d.stream)
	if err != nil {
		return err
	}
	topic, err := identifier(topicName)
	if err != nil {
		return err
	}
	group, err := identifier(groupName)
	if err != nil {
		return err
	}
	if _, err := d.client.GetConsumerGroup(ctx, stream, topic, group); err != nil {
		if _, createErr := d.client.CreateConsumerGroup(ctx, stream, topic, groupName); createErr != nil {
			if _, getErr := d.client.GetConsumerGroup(ctx, stream, topic, group); getErr != nil {
				return createErr
			}
		}
	}
	return d.client.JoinConsumerGroup(ctx, stream, topic, group)
}

func (d *SDKDriver) Leave(ctx context.Context, topicName, groupName string) error {
	stream, err := identifier(d.stream)
	if err != nil {
		return err
	}
	topic, err := identifier(topicName)
	if err != nil {
		return err
	}
	group, err := identifier(groupName)
	if err != nil {
		return err
	}
	return d.client.LeaveConsumerGroup(ctx, stream, topic, group)
}

func (d *SDKDriver) Poll(ctx context.Context, topicName, groupName string, count int) ([]Message, error) {
	stream, err := identifier(d.stream)
	if err != nil {
		return nil, err
	}
	topic, err := identifier(topicName)
	if err != nil {
		return nil, err
	}
	group, err := identifier(groupName)
	if err != nil {
		return nil, err
	}
	polled, err := d.client.PollMessages(ctx, stream, topic, iggcon.NewGroupConsumer(group), iggcon.NextPollingStrategy(), uint32(count), false, nil)
	if err != nil {
		return nil, err
	}
	if polled == nil {
		return nil, nil
	}
	messages := make([]Message, 0, len(polled.Messages))
	for _, message := range polled.Messages {
		messages = append(messages, Message{Payload: message.Payload, Offset: message.Header.Offset, Partition: polled.PartitionId})
	}
	return messages, nil
}

func (d *SDKDriver) Commit(ctx context.Context, topicName, groupName string, partition uint32, offset uint64) error {
	stream, err := identifier(d.stream)
	if err != nil {
		return err
	}
	topic, err := identifier(topicName)
	if err != nil {
		return err
	}
	group, err := identifier(groupName)
	if err != nil {
		return err
	}
	return d.client.StoreConsumerOffset(ctx, iggcon.NewGroupConsumer(group), stream, topic, offset, &partition)
}

func (d *SDKDriver) Ping(ctx context.Context) error { return d.client.Ping(ctx) }
func (d *SDKDriver) Close() error                   { return d.client.Close() }

var _ Driver = (*SDKDriver)(nil)
