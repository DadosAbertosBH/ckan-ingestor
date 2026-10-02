// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package iggytransport

import (
	"context"
	"errors"
	"testing"
)

type testDriver struct {
	commits      int
	leave        int
	joinAttempts int
	joinFailures int
	cancel       context.CancelFunc
}

func (d *testDriver) Join(context.Context, string, string) error {
	d.joinAttempts++
	if d.joinAttempts <= d.joinFailures {
		return errors.New("stream with id: 0 was not found")
	}
	return nil
}
func (d *testDriver) Leave(context.Context, string, string) error { d.leave++; return nil }
func (d *testDriver) Poll(context.Context, string, string, int) ([]Message, error) {
	return []Message{{Payload: []byte(`{}`), Offset: 12, Partition: 2}}, nil
}
func (d *testDriver) Commit(context.Context, string, string, uint32, uint64) error {
	d.commits++
	d.cancel()
	return nil
}
func TestConsumptionCommitsAfterHandlerAndPreservesFailuresForReplay(t *testing.T) {
	for _, fail := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		d := &testDriver{cancel: cancel}
		calls := 0
		err := (Consumer{Driver: d}).Consume(ctx, "sync", "group", func([]byte) error {
			calls++
			if fail {
				return errors.New("DB failed")
			}
			return nil
		})
		cancel()
		if calls != 1 || d.leave != 1 {
			t.Fatalf("calls=%d leave=%d", calls, d.leave)
		}
		if fail && (err == nil || d.commits != 0) {
			t.Fatal("failed sync was committed")
		}
		if !fail && d.commits != 1 {
			t.Fatal("successful sync was not committed")
		}
	}
}

func TestConsumptionRetriesJoinUntilIggyTopologyExists(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := &testDriver{cancel: cancel, joinFailures: 1}
	err := (Consumer{Driver: d}).Consume(ctx, "sync", "group", func([]byte) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if d.joinAttempts != 2 || d.commits != 1 {
		t.Fatalf("join attempts=%d commits=%d", d.joinAttempts, d.commits)
	}
}
