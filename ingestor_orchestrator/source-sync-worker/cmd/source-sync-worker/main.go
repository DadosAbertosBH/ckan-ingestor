// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package main

import (
	"context"
	worker "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/source-sync-worker"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := worker.Run(ctx, worker.LoadConfig()); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
