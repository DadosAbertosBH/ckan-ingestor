// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package iggytransport

import (
	"context"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

func (d *Client) Join(ctx context.Context, topicName, groupName string) error {
	stream := d.stream
	topic, err := iggcon.NewIdentifier(topicName)
	if err != nil {
		return err
	}
	group, err := iggcon.NewIdentifier(groupName)
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

func (d *Client) Leave(ctx context.Context, topicName, groupName string) error {
	stream := d.stream
	topic, err := iggcon.NewIdentifier(topicName)
	if err != nil {
		return err
	}
	group, err := iggcon.NewIdentifier(groupName)
	if err != nil {
		return err
	}
	return d.client.LeaveConsumerGroup(ctx, stream, topic, group)
}

func (d *Client) Poll(ctx context.Context, topicName, groupName string, count int) ([]Message, error) {
	stream := d.stream
	topic, err := iggcon.NewIdentifier(topicName)
	if err != nil {
		return nil, err
	}
	group, err := iggcon.NewIdentifier(groupName)
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

func (d *Client) Commit(ctx context.Context, topicName, groupName string, partition uint32, offset uint64) error {
	stream := d.stream
	topic, err := iggcon.NewIdentifier(topicName)
	if err != nil {
		return err
	}
	group, err := iggcon.NewIdentifier(groupName)
	if err != nil {
		return err
	}
	return d.client.StoreConsumerOffset(ctx, iggcon.NewGroupConsumer(group), stream, topic, offset, &partition)
}
