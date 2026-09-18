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
	"fmt"

	"github.com/apache/iggy/foreign/go/client"
	"github.com/apache/iggy/foreign/go/client/tcp"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/backend/internal/config"
)

type SDKDriver struct {
	client iggcon.Client
	stream string
}

func NewSDKDriver(cfg config.Config) (*SDKDriver, error) {
	cli, err := client.NewIggyClient(client.WithTcp(tcp.WithServerAddress(cfg.IggyAddress)))
	if err != nil {
		return nil, err
	}
	if _, err := cli.LoginUser(cfg.IggyUsername, cfg.IggyPassword); err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("login to Iggy: %w", err)
	}
	return &SDKDriver{client: cli, stream: cfg.Stream}, nil
}

func identifier(value string) (iggcon.Identifier, error) { return iggcon.NewIdentifier(value) }

func (d *SDKDriver) EnsureStream(name string) error {
	id, err := identifier(name)
	if err != nil {
		return err
	}
	if _, err := d.client.GetStream(id); err == nil {
		return nil
	}
	if _, err := d.client.CreateStream(name); err != nil {
		if _, getErr := d.client.GetStream(id); getErr != nil {
			return err
		}
	}
	return nil
}

func (d *SDKDriver) EnsureTopic(name string, partitions int) error {
	stream, err := identifier(d.stream)
	if err != nil {
		return err
	}
	topic, err := identifier(name)
	if err != nil {
		return err
	}
	if _, err := d.client.GetTopic(stream, topic); err == nil {
		return nil
	}
	replication := uint8(1)
	if _, err := d.client.CreateTopic(stream, name, uint32(partitions), 0, iggcon.IggyExpiryServerDefault, 0, &replication); err != nil {
		if _, getErr := d.client.GetTopic(stream, topic); getErr != nil {
			return err
		}
	}
	return nil
}

func (d *SDKDriver) Send(topicName string, partition int, payload []byte) error {
	stream, err := identifier(d.stream)
	if err != nil {
		return err
	}
	topic, err := identifier(topicName)
	if err != nil {
		return err
	}
	message, err := iggcon.NewIggyMessage(payload)
	if err != nil {
		return err
	}
	return d.client.SendMessages(stream, topic, iggcon.PartitionId(uint32(partition)), []iggcon.IggyMessage{message})
}

func (d *SDKDriver) Join(topicName, groupName string) error {
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
	if _, err := d.client.GetConsumerGroup(stream, topic, group); err != nil {
		if _, createErr := d.client.CreateConsumerGroup(stream, topic, groupName); createErr != nil {
			if _, getErr := d.client.GetConsumerGroup(stream, topic, group); getErr != nil {
				return createErr
			}
		}
	}
	return d.client.JoinConsumerGroup(stream, topic, group)
}

func (d *SDKDriver) Poll(topicName, groupName string, count int) ([]Message, error) {
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
	polled, err := d.client.PollMessages(stream, topic, iggcon.NewGroupConsumer(group), iggcon.NextPollingStrategy(), uint32(count), false, nil)
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

func (d *SDKDriver) Commit(topicName, groupName string, partition uint32, offset uint64) error {
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
	return d.client.StoreConsumerOffset(iggcon.NewGroupConsumer(group), stream, topic, offset, &partition)
}

func (d *SDKDriver) Ping() error  { return d.client.Ping() }
func (d *SDKDriver) Close() error { return d.client.Close() }

var _ Driver = (*SDKDriver)(nil)
