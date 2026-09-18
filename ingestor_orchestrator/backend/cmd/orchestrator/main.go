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

package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/backend/internal/app"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/backend/internal/config"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/backend/internal/httpapi"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/backend/internal/iggy"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/backend/internal/migrations"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/backend/internal/mysqlstore"
	runtimeapp "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/backend/internal/runtime"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		if err := migrate(); err != nil {
			slog.Error("migration failed", "error", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "wait" {
		if err := waitForReady(); err != nil {
			slog.Error("readiness wait failed", "error", err)
			os.Exit(1)
		}
		return
	}
	if err := run(); err != nil {
		slog.Error("orchestrator stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	store, err := mysqlstore.Open(cfg.MySQLDSN)
	if err != nil {
		return err
	}
	defer store.DB.Close()
	if err := retryUntil(ctx, cfg.StartupTimeout, store.Ping); err != nil {
		return fmt.Errorf("connect to MySQL: %w", err)
	}

	var driver *iggy.SDKDriver
	err = retryUntil(ctx, cfg.StartupTimeout, func(context.Context) error {
		created, createErr := iggy.NewSDKDriver(cfg)
		if createErr == nil {
			driver = created
		}
		return createErr
	})
	if err != nil {
		return fmt.Errorf("connect to Iggy: %w", err)
	}
	bus := iggy.NewBus(cfg, driver)
	defer bus.Close()
	if err := retryUntil(ctx, cfg.StartupTimeout, bus.EnsureTopology); err != nil {
		return fmt.Errorf("initialize Iggy topology: %w", err)
	}

	processor := &app.Processor{Store: store, NewID: uuid.NewString}
	dispatcher := &app.Dispatcher{Store: store, Publisher: bus, RetryTopic: cfg.RetryTopic, MetadataTopic: cfg.MetadataTopic, NewID: uuid.NewString}
	consumers := runtimeapp.NewConsumers(bus, processor, cfg)
	consumers.Start(ctx)
	scheduler := &runtimeapp.Scheduler{Locker: store, Dispatcher: dispatcher, Interval: cfg.SchedulerInterval}
	go scheduler.Run(ctx)

	server := &http.Server{Addr: cfg.HTTPAddress, Handler: httpapi.New(dispatcher, store, bus, consumers.Ready, httpapi.WithQueries(store), httpapi.WithStaticDir(cfg.StaticDir)), ReadHeaderTimeout: 5 * time.Second}
	errCh := make(chan error, 1)
	go func() { errCh <- server.ListenAndServe() }()
	slog.Info("Go API started", "address", cfg.HTTPAddress)
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-errCh:
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	}
}

func migrate() error {
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), cfg.StartupTimeout)
	defer cancel()
	store, err := mysqlstore.Open(cfg.MySQLDSN)
	if err != nil {
		return err
	}
	defer store.DB.Close()
	if err := retryUntil(ctx, cfg.StartupTimeout, store.Ping); err != nil {
		return fmt.Errorf("connect to MySQL: %w", err)
	}
	return migrations.Up(ctx, store.DB)
}

func retryUntil(ctx context.Context, timeout time.Duration, operation func(context.Context) error) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		if lastErr = operation(ctx); lastErr == nil {
			return nil
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return lastErr
}

func waitForReady() error {
	if len(os.Args) < 3 {
		return fmt.Errorf("usage: orchestrator-go wait URL")
	}
	deadline := time.Now().Add(config.Load().StartupTimeout)
	client := &http.Client{Timeout: 2 * time.Second}
	for time.Now().Before(deadline) {
		response, err := client.Get(os.Args[2])
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s", os.Args[2])
}
