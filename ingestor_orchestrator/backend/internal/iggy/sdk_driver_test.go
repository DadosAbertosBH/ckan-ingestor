// Noctcloud Desenvolvimento LTDA
// Copyright (C) 2026  Noctcloud Desenvolvimento LTDA
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package iggy

import (
	"context"
	"errors"
	"os"
	"testing"

	iggycontracts "github.com/apache/iggy/foreign/go/contracts"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/backend/internal/config"
)

type topicClientFake struct {
	topicExists  bool
	streamTopics []iggycontracts.Topic
	updatedNames []string
	updateErr    error
	created      struct {
		expiry iggycontracts.Duration
		size   uint64
	}
	updated struct {
		expiry iggycontracts.Duration
		size   uint64
	}
}

func (f *topicClientFake) GetStream(context.Context, iggycontracts.Identifier) (*iggycontracts.StreamDetails, error) {
	if f.streamTopics != nil {
		return &iggycontracts.StreamDetails{Topics: f.streamTopics}, nil
	}
	return nil, errors.New("not found")
}
func (f *topicClientFake) CreateStream(context.Context, string) (*iggycontracts.StreamDetails, error) {
	return nil, nil
}
func (f *topicClientFake) GetTopic(context.Context, iggycontracts.Identifier, iggycontracts.Identifier) (*iggycontracts.TopicDetails, error) {
	if f.topicExists {
		return &iggycontracts.TopicDetails{}, nil
	}
	return nil, errors.New("not found")
}
func (f *topicClientFake) CreateTopic(_ context.Context, _ iggycontracts.Identifier, _ string, _ uint32, _ iggycontracts.CompressionAlgorithm, expiry iggycontracts.Duration, size uint64, _ ...iggycontracts.HeaderEntry) (*iggycontracts.TopicDetails, error) {
	f.created.expiry, f.created.size = expiry, size
	return &iggycontracts.TopicDetails{}, nil
}
func (f *topicClientFake) UpdateTopic(_ context.Context, _ iggycontracts.Identifier, _ iggycontracts.Identifier, name string, _ iggycontracts.CompressionAlgorithm, expiry iggycontracts.Duration, size uint64, _ ...iggycontracts.HeaderEntry) error {
	f.updatedNames = append(f.updatedNames, name)
	f.updated.expiry, f.updated.size = expiry, size
	return f.updateErr
}
func (f *topicClientFake) Ping(context.Context) error { return nil }

func TestEnsureStreamAppliesRetentionToWorkerOwnedTopics(t *testing.T) {
	client := &topicClientFake{streamTopics: []iggycontracts.Topic{
		{Name: "jobs-retry", PartitionsCount: 3},
		{Name: "parquet-results", PartitionsCount: 1},
		{Name: "job-results", MessageExpiry: messageExpiry, MaxTopicSize: maxTopicSize},
	}}
	driver := &SDKDriver{client: client, stream: "ckan-ingestor"}
	if err := driver.EnsureStream(context.Background(), "ckan-ingestor"); err != nil {
		t.Fatal(err)
	}
	if len(client.updatedNames) != 2 || client.updatedNames[0] != "jobs-retry" || client.updatedNames[1] != "parquet-results" {
		t.Fatalf("updated topics = %v", client.updatedNames)
	}
	if client.updated.expiry != messageExpiry || client.updated.size != maxTopicSize {
		t.Fatalf("retention = %#v", client.updated)
	}
}

func TestEnsureStreamPropagatesRetentionFailure(t *testing.T) {
	want := errors.New("broker unavailable")
	client := &topicClientFake{streamTopics: []iggycontracts.Topic{{Name: "parquet-results"}}, updateErr: want}
	driver := &SDKDriver{client: client, stream: "ckan-ingestor"}
	if err := driver.EnsureStream(context.Background(), "ckan-ingestor"); !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}

func TestEnsureTopicAppliesRetentionToNewAndExistingTopics(t *testing.T) {
	wantExpiry := iggycontracts.Duration(30 * 24 * iggycontracts.Hour)
	const wantSize = uint64(2 * 1024 * 1024 * 1024)

	for _, item := range []struct {
		name   string
		exists bool
	}{
		{name: "new"},
		{name: "existing", exists: true},
	} {
		t.Run(item.name, func(t *testing.T) {
			client := &topicClientFake{topicExists: item.exists}
			driver := &SDKDriver{client: client, stream: "ckan-ingestor"}
			if err := driver.EnsureTopic(context.Background(), "jobs", 1); err != nil {
				t.Fatal(err)
			}
			if item.exists {
				if client.updated.expiry != wantExpiry || client.updated.size != wantSize {
					t.Fatalf("updated retention = (%d, %d), want (%d, %d)", client.updated.expiry, client.updated.size, wantExpiry, wantSize)
				}
				return
			}
			if client.created.expiry != wantExpiry || client.created.size != wantSize {
				t.Fatalf("created retention = (%d, %d), want (%d, %d)", client.created.expiry, client.created.size, wantExpiry, wantSize)
			}
		})
	}
}

func TestSDKDriverAgainstIggy(t *testing.T) {
	address := os.Getenv("IGGY_INTEGRATION_ADDRESS")
	if address == "" {
		t.Skip("set IGGY_INTEGRATION_ADDRESS to run against Iggy")
	}
	ctx := context.Background()
	driver, err := NewSDKDriver(ctx, config.Config{IggyAddress: address, IggyUsername: "iggy", IggyPassword: "iggy", Stream: "ckan-ingestor"})
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()
	if err := driver.EnsureStream(ctx, "ckan-ingestor"); err != nil {
		t.Fatalf("stream: %v", err)
	}
	for _, topic := range []string{"jobs", "jobs-retry", "job-results", "ckan_metadata_sync", "ckan_metadata_sync_result"} {
		if err := driver.EnsureTopic(ctx, topic, 1); err != nil {
			t.Fatalf("topic %s: %v", topic, err)
		}
	}
}
