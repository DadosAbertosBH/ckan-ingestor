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
	"errors"
	"testing"
	"time"

	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/backend/internal/config"
)

type fakeDriver struct {
	streams []string
	topics  map[string]int
	sends   []struct {
		topic     string
		partition int
		payload   []byte
	}
	messages   []Message
	commits    []Message
	leaves     []struct{ topic, group string }
	handlerErr error
}

func (f *fakeDriver) EnsureStream(_ context.Context, value string) error {
	f.streams = append(f.streams, value)
	return nil
}
func (f *fakeDriver) EnsureTopic(_ context.Context, value string, count int) error {
	if f.topics == nil {
		f.topics = map[string]int{}
	}
	f.topics[value] = count
	return nil
}
func (f *fakeDriver) Send(_ context.Context, topic string, partition int, payload []byte) error {
	f.sends = append(f.sends, struct {
		topic     string
		partition int
		payload   []byte
	}{topic, partition, payload})
	return nil
}
func (f *fakeDriver) Join(context.Context, string, string) error { return nil }
func (f *fakeDriver) Leave(_ context.Context, topic, group string) error {
	f.leaves = append(f.leaves, struct{ topic, group string }{topic, group})
	return nil
}
func (f *fakeDriver) Poll(context.Context, string, string, int) ([]Message, error) {
	values := f.messages
	f.messages = nil
	return values, nil
}
func (f *fakeDriver) Commit(_ context.Context, _ string, _ string, partition uint32, offset uint64) error {
	f.commits = append(f.commits, Message{Partition: partition, Offset: offset})
	return nil
}
func (f *fakeDriver) Ping(context.Context) error { return nil }
func (f *fakeDriver) Close() error               { return nil }

func busConfig() config.Config {
	return config.Config{Stream: "ckan-ingestor", JobTopic: "jobs", RetryTopic: "jobs-retry", ResultTopic: "job-results", MetadataTopic: "metadata", MetadataResultTopic: "metadata-results", JobPartitions: 10, RetryPartitions: 4, ResultPartitions: 3, PollInterval: time.Millisecond}
}

func TestEnsureTopologyCreatesAllTopics(t *testing.T) {
	driver := &fakeDriver{}
	bus := NewBus(busConfig(), driver)
	if err := bus.EnsureTopology(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(driver.streams) != 1 || len(driver.topics) != 5 {
		t.Fatalf("topology = %#v %#v", driver.streams, driver.topics)
	}
	if driver.topics["jobs-retry"] != 4 || driver.topics["metadata"] != 1 {
		t.Fatalf("topics = %#v", driver.topics)
	}
}

func TestPublishRoutesDeterministicallyByDestinationCount(t *testing.T) {
	driver := &fakeDriver{}
	bus := NewBus(busConfig(), driver)
	first, err := bus.Publish(context.Background(), "jobs-retry", []byte("one"), "stable")
	if err != nil {
		t.Fatal(err)
	}
	second, _ := bus.Publish(context.Background(), "jobs-retry", []byte("two"), "stable")
	metadata, _ := bus.Publish(context.Background(), "metadata", []byte("three"), "stable")
	if first.Partition != second.Partition || first.Partition >= 4 || metadata.Partition != 0 {
		t.Fatalf("routing = %#v %#v %#v", first, second, metadata)
	}
}

func TestConsumeCommitsOnlyAfterSuccessfulHandler(t *testing.T) {
	driver := &fakeDriver{messages: []Message{{Payload: []byte("ok"), Offset: 42, Partition: 3}}}
	bus := NewBus(busConfig(), driver)
	ctx, cancel := context.WithCancel(context.Background())
	err := bus.Consume(ctx, "job-results", "group", func([]byte) error { cancel(); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if len(driver.commits) != 1 || driver.commits[0].Offset != 42 {
		t.Fatalf("commits = %#v", driver.commits)
	}

	driver.messages = []Message{{Payload: []byte("bad"), Offset: 43, Partition: 3}}
	err = bus.Consume(context.Background(), "job-results", "group", func([]byte) error { return errors.New("db down") })
	if err == nil || len(driver.commits) != 1 {
		t.Fatalf("error = %v commits = %#v", err, driver.commits)
	}
	if len(driver.leaves) != 2 {
		t.Fatalf("leaves = %#v", driver.leaves)
	}
}

func TestConsumeLeavesGroupWhenContextIsCancelledWhileIdle(t *testing.T) {
	driver := &fakeDriver{}
	bus := NewBus(busConfig(), driver)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := bus.Consume(ctx, "job-results", "result-consumer", func([]byte) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if len(driver.leaves) != 1 || driver.leaves[0].topic != "job-results" || driver.leaves[0].group != "result-consumer" {
		t.Fatalf("leaves = %#v", driver.leaves)
	}
}
