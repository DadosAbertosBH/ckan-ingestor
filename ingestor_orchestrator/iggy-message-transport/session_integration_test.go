// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.

package iggytransport

import (
	"context"
	"os"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/google/uuid"
)

// Run against the isolated candidate before upgrading the shared production broker.
func TestSessionBrokerCompatibility(t *testing.T) {
	address := os.Getenv("IGGY_INTEGRATION_ADDRESS")
	if address == "" {
		t.Skip("set IGGY_INTEGRATION_ADDRESS to the isolated session-recovery broker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	streamName := "go-session-" + uuid.NewString()
	transport, err := Connect(ctx, ConnectionConfig{Address: address, Username: "iggy", Password: "session-test-password", Stream: streamName})
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	if _, err := transport.client.CreateStream(ctx, streamName); err != nil {
		t.Fatal(err)
	}
	if _, err := transport.client.CreateTopic(ctx, transport.stream, "results", 1, 0, iggcon.Duration(iggcon.Hour), 0); err != nil {
		t.Fatal(err)
	}
	if err := transport.Join(ctx, "results", "go-coordinator"); err != nil {
		t.Fatal(err)
	}
	if _, err := transport.Send(ctx, "results", uuid.NewString(), []byte(`{"probe":"go"}`)); err != nil {
		t.Fatal(err)
	}
	var message Message
	for {
		messages, err := transport.Poll(ctx, "results", "go-coordinator", 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(messages) > 0 {
			message = messages[0]
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("Go consumer did not receive the candidate broker's message")
		case <-time.After(25 * time.Millisecond):
		}
	}
	if string(message.Payload) != `{"probe":"go"}` {
		t.Fatalf("unexpected payload: %s", message.Payload)
	}
	if err := transport.Commit(ctx, "results", "go-coordinator", message.Partition, message.Offset); err != nil {
		t.Fatal(err)
	}
	if err := transport.Leave(ctx, "results", "go-coordinator"); err != nil {
		t.Fatal(err)
	}
	if err := transport.Join(ctx, "results", "go-coordinator"); err != nil {
		t.Fatal(err)
	}
	messages, err := transport.Poll(ctx, "results", "go-coordinator", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 0 {
		t.Fatalf("confirmed message replayed after Go consumer rejoined: %+v", messages)
	}
	if err := transport.Leave(ctx, "results", "go-coordinator"); err != nil {
		t.Fatal(err)
	}
}
