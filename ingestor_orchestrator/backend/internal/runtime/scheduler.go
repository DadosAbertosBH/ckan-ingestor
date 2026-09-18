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
	"context"
	"log/slog"
	"time"

	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/backend/internal/app"
)

type SchedulerLocker interface {
	WithSchedulerLock(context.Context, func() error) error
}
type SyncAllDispatcher interface {
	SyncAll(context.Context) []app.SyncDispatchResult
}
type Scheduler struct {
	Locker     SchedulerLocker
	Dispatcher SyncAllDispatcher
	Interval   time.Duration
}

func (s *Scheduler) Run(ctx context.Context) {
	s.runOnce(ctx)
	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.runOnce(ctx)
		}
	}
}
func (s *Scheduler) runOnce(ctx context.Context) {
	if err := s.Locker.WithSchedulerLock(ctx, func() error {
		results := s.Dispatcher.SyncAll(ctx)
		for _, result := range results {
			if result.Error != "" {
				slog.Error("metadata sync dispatch failed", "instance_id", result.InstanceID, "error", result.Error)
			}
		}
		return nil
	}); err != nil {
		slog.Error("metadata scheduler failed", "error", err)
	}
}
