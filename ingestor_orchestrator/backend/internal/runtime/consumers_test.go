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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/backend/internal/app"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/backend/internal/config"
)

type fakeProcessor struct {
	jobs     []app.JobResultMessage
	metadata []app.MetadataSyncResultMessage
	err      error
}

type blockingConsumer struct{ calls atomic.Int32 }

func (c *blockingConsumer) Consume(ctx context.Context, _ string, _ string, _ func([]byte) error) error {
	c.calls.Add(1)
	<-ctx.Done()
	return nil
}

func (f *fakeProcessor) ApplyJobResult(_ context.Context, value app.JobResultMessage) error {
	f.jobs = append(f.jobs, value)
	return f.err
}
func (f *fakeProcessor) ApplyMetadataResult(_ context.Context, value app.MetadataSyncResultMessage) error {
	f.metadata = append(f.metadata, value)
	return f.err
}

func TestHandlersDecodeContracts(t *testing.T) {
	processor := &fakeProcessor{}
	consumers := &Consumers{processor: processor}
	if err := consumers.handleJob(context.Background(), []byte(`{"job_id":"job","resource_id":"resource","status":"FAILED","http_status":403}`)); err != nil {
		t.Fatal(err)
	}
	if err := consumers.handleMetadata(context.Background(), []byte(`{"sync_id":"sync","status":"success"}`)); err != nil {
		t.Fatal(err)
	}
	if len(processor.jobs) != 1 || processor.jobs[0].JobID != "job" || processor.jobs[0].HTTPStatus == nil || *processor.jobs[0].HTTPStatus != 403 || len(processor.metadata) != 1 {
		t.Fatalf("decoded = %#v %#v", processor.jobs, processor.metadata)
	}
}

func TestMalformedMessageAndProcessingFailureAreReturned(t *testing.T) {
	consumers := &Consumers{processor: &fakeProcessor{err: errors.New("db down")}}
	if err := consumers.handleJob(context.Background(), []byte(`{`)); err == nil {
		t.Fatal("expected decode error")
	}
	if err := consumers.handleMetadata(context.Background(), []byte(`{"sync_id":"sync","status":"success"}`)); err == nil {
		t.Fatal("expected processing error")
	}
}

func TestJobHandlerRetriesNotFoundAndStopsWhenContextIsCancelled(t *testing.T) {
	processor := &retryProcessor{remaining: 2}
	consumers := &Consumers{processor: processor, cfg: config.Config{ResultNotFoundRetryBackoffEnabled: true}}
	if err := consumers.handleJob(context.Background(), []byte(`{"job_id":"job","resource_id":"resource","status":"SUCCESS"}`)); err != nil {
		t.Fatal(err)
	}
	if processor.calls != 3 {
		t.Fatalf("calls = %d", processor.calls)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	processor.remaining = 5
	if err := consumers.handleJob(ctx, []byte(`{"job_id":"job","resource_id":"resource","status":"SUCCESS"}`)); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
}

func TestJobHandlerSkipsUnknownJobWithoutRetryBackoff(t *testing.T) {
	processor := &retryProcessor{remaining: 5}
	consumers := &Consumers{processor: processor, cfg: config.Config{ResultNotFoundRetryBackoffEnabled: false}}
	started := time.Now()
	if err := consumers.handleJob(context.Background(), []byte(`{"job_id":"job","resource_id":"resource","status":"SUCCESS"}`)); err != nil {
		t.Fatal(err)
	}
	if processor.calls != 1 {
		t.Fatalf("calls = %d, want 1", processor.calls)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("retries took %s, want no backoff", elapsed)
	}
}

func TestJobHandlerSkipsRemovedInstanceOnceAndLogsItsIdentity(t *testing.T) {
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	processor := &fakeProcessor{err: fmt.Errorf("lookup: %w", app.ErrInstanceNotFound)}
	consumers := &Consumers{processor: processor}
	if err := consumers.handleJob(context.Background(), []byte(`{"job_id":"orphan","resource_id":"resource","instance_id":"removed-instance","status":"PENDING"}`)); err != nil {
		t.Fatalf("handler must allow offset commit: %v", err)
	}
	if len(processor.jobs) != 1 {
		t.Fatalf("processor called %d times, want 1", len(processor.jobs))
	}
	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record["msg"] != "job result skipped: instance no longer exists" || record["level"] != "WARN" || record["job_id"] != "orphan" || record["instance_id"] != "removed-instance" || record["status"] != "PENDING" {
		t.Fatalf("log = %#v", record)
	}
}

func TestJobHandlerPreservesDatabaseFailures(t *testing.T) {
	want := errors.New("database unavailable")
	processor := &fakeProcessor{err: want}
	consumers := &Consumers{processor: processor}
	if err := consumers.handleJob(context.Background(), []byte(`{"job_id":"job","resource_id":"resource","status":"PENDING"}`)); !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
	if len(processor.jobs) != 1 {
		t.Fatalf("processor called %d times, want 1", len(processor.jobs))
	}
}

func TestWaitBlocksUntilConsumerGoroutinesStop(t *testing.T) {
	bus := &blockingConsumer{}
	consumers := &Consumers{bus: bus, processor: &fakeProcessor{}, cfg: config.Config{
		ResultTopic: "job-results", ResultGroup: "job-results-consumer",
		MetadataResultTopic: "metadata-results", MetadataResultGroup: "metadata-results-consumer",
	}}
	waiter, ok := any(consumers).(interface{ Wait() })
	if !ok {
		t.Fatal("Consumers must expose Wait")
	}
	ctx, cancel := context.WithCancel(context.Background())
	consumers.Start(ctx)
	deadline := time.After(time.Second)
	for bus.calls.Load() != 2 {
		select {
		case <-deadline:
			t.Fatal("consumers did not start")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	waiter.Wait()
	if consumers.Ready() {
		t.Fatal("consumers remained active after Wait")
	}
}

type retryProcessor struct{ remaining, calls int }

func (p *retryProcessor) ApplyJobResult(context.Context, app.JobResultMessage) error {
	p.calls++
	if p.remaining > 0 {
		p.remaining--
		return app.ErrNotFound
	}
	return nil
}
func (p *retryProcessor) ApplyMetadataResult(context.Context, app.MetadataSyncResultMessage) error {
	return nil
}

type fakeLocker struct{ calls atomic.Int32 }

func (f *fakeLocker) WithSchedulerLock(_ context.Context, fn func() error) error {
	f.calls.Add(1)
	return fn()
}

type fakeSyncDispatcher struct{ calls atomic.Int32 }

func (f *fakeSyncDispatcher) SyncAll(context.Context) []app.SyncDispatchResult {
	f.calls.Add(1)
	return []app.SyncDispatchResult{{InstanceID: "instance", Error: "broker unavailable"}}
}

func TestSchedulerRunsImmediatelyAndStopsOnCancellation(t *testing.T) {
	locker, dispatcher := &fakeLocker{}, &fakeSyncDispatcher{}
	scheduler := &Scheduler{Locker: locker, Dispatcher: dispatcher, Interval: time.Hour}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { scheduler.Run(ctx); close(done) }()
	deadline := time.After(time.Second)
	for dispatcher.calls.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("scheduler did not dispatch immediately")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("scheduler did not stop")
	}
	if locker.calls.Load() != 1 {
		t.Fatalf("locks = %d", locker.calls.Load())
	}
}
