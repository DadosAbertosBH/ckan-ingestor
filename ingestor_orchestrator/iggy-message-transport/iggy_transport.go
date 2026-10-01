// Noctcloud Desenvolvimento LTDA
// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
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
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

// Package iggytransport provides authenticated, UUID-routed and confirmed Iggy message delivery.
package iggytransport

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/iggy/foreign/go/client"
	"github.com/apache/iggy/foreign/go/client/tcp"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/google/uuid"
)

type ConnectionConfig struct{ Address, Username, Password, Stream string }

var ErrInvalidRoutingKey = errors.New("Iggy routing key must be a UUID")

type Client struct {
	client iggcon.Client
	stream iggcon.Identifier
}

func Connect(ctx context.Context, cfg ConnectionConfig) (*Client, error) {
	stream, err := iggcon.NewIdentifier(cfg.Stream)
	if err != nil {
		return nil, fmt.Errorf("create Iggy stream identifier: %w", err)
	}
	client, err := client.NewIggyClient(client.WithTcp(tcp.WithServerAddress(cfg.Address), tcp.WithAutoLogin(tcp.NewUsernamePasswordCredentials(cfg.Username, cfg.Password))))
	if err != nil {
		return nil, fmt.Errorf("create Iggy client: %w", err)
	}
	if err := client.Connect(ctx); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("connect to Iggy: %w", err)
	}
	return &Client{client: client, stream: stream}, nil
}
func (c *Client) Send(ctx context.Context, topicName, key string, payload []byte) (int, error) {
	topic, err := iggcon.NewIdentifier(topicName)
	if err != nil {
		return 0, fmt.Errorf("create Iggy topic identifier: %w", err)
	}
	routingID, err := uuid.Parse(key)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrInvalidRoutingKey, err)
	}
	message, err := iggcon.NewIggyMessage(payload)
	if err != nil {
		return 0, fmt.Errorf("create Iggy message: %w", err)
	}
	response, err := c.client.SendMessages(ctx, c.stream, topic, iggcon.EntityIdGuid(routingID), []iggcon.IggyMessage{message})
	if err != nil {
		return 0, fmt.Errorf("send Iggy message: %w", err)
	}
	if response == nil || len(response.Confirmations) == 0 {
		return 0, errors.New("Iggy did not confirm message routing")
	}
	return int(response.Confirmations[0].PartitionId), nil
}
func (c *Client) Close() error {
	if c == nil || c.client == nil {
		return nil
	}
	return c.client.Close()
}

// RawClient serves topology and consumer operations owned by the backend.
func (c *Client) RawClient() iggcon.Client            { return c.client }
func (c *Client) StreamIdentifier() iggcon.Identifier { return c.stream }
