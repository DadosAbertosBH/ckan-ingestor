// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// SPDX-License-Identifier: AGPL-3.0-or-later
// Licensed under the GNU Affero General Public License, version 3 or later.
package oraclerepositories

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	go_ora "github.com/sijms/go-ora/v2"
	r "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/repository-contracts"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/repository-contracts/contracttest"
)

func TestOracleDockerSizing(t *testing.T) {
	if os.Getenv("ORACLE_DOCKER_TEST") != "1" {
		t.Skip("set ORACLE_DOCKER_TEST=1 to size Autonomous Free in Docker")
	}
	image := os.Getenv("ORACLE_TEST_IMAGE")
	if image == "" {
		image = "ghcr.io/oracle/adb-free:26.9.4.1-26ai"
	}
	limits := []string{"256m", "512m", "1g", "2g", "4g", "6g", "8g"}
	if from := os.Getenv("ORACLE_TEST_MEMORY"); from != "" {
		limits = []string{from}
	}
	for _, limit := range limits {
		viable := false
		t.Run(limit, func(t *testing.T) {
			name := "ckan-oracle-sizing-" + uuid.NewString()
			wallet := t.TempDir()
			docker(t, "run", "-d", "--name", name, "--hostname", "localhost", "--cpus=4", "--memory="+limit, "--memory-swap="+limit, "--cap-add=SYS_ADMIN", "--device=/dev/fuse", "-p", "127.0.0.1::1522", "-e", "WORKLOAD_TYPE=ATP", "-e", "ADMIN_PASSWORD=ContractAdmin123", "-e", "WALLET_PASSWORD=ContractWallet123", image)
			t.Cleanup(func() { docker(t, "rm", "-f", "-v", name) })
			started := time.Now()
			deadline := started.Add(20 * time.Minute)
			ready := false
			for time.Now().Before(deadline) {
				state := docker(t, "inspect", "--format", "{{json .State}}", name)
				var status struct {
					Running, OOMKilled bool
					ExitCode           int
				}
				if err := json.Unmarshal([]byte(state), &status); err != nil {
					t.Fatal(err)
				}
				if !status.Running {
					t.Logf("memory=%s startup_failed state=%s logs=%s", limit, state, docker(t, "logs", "--tail", "15", name))
					return
				}
				// Wallet creation and SQL readiness are separate steps during startup.
				probeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				probe := exec.CommandContext(probeCtx, "docker", "exec", name, "bash", "-c", "echo 'select 1 from dual;' | sqlplus -L -S 'admin/ContractAdmin123@myatp_tp'")
				output, err := probe.CombinedOutput()
				cancel()
				if err == nil && !strings.Contains(string(output), "ORA-") && regexp.MustCompile(`(?m)^\s*1\s*$`).Match(output) {
					ready = true
					break
				}
				time.Sleep(5 * time.Second)
			}
			if !ready {
				t.Logf("memory=%s startup_timeout logs=%s", limit, docker(t, "logs", "--tail", "15", name))
				return
			}
			docker(t, "cp", name+":/u01/app/oracle/wallets/tls_wallet/.", wallet)
			tns, err := os.ReadFile(filepath.Join(wallet, "tnsnames.ora"))
			if err != nil {
				t.Fatal(err)
			}
			service, err := walletService(tns)
			if err != nil {
				t.Fatal(err)
			}
			portText := strings.TrimSpace(docker(t, "port", name, "1522/tcp"))
			port, err := strconv.Atoi(strings.TrimPrefix(portText, "127.0.0.1:"))
			if err != nil {
				t.Fatal(err)
			}
			options := map[string]string{"SSL": "true", "SSL VERIFY": "true", "WALLET": wallet, "TIMEOUT": "10", "CONNECTION TIMEOUT": "5"}
			admin, err := Open(go_ora.BuildUrl("localhost", port, service, "admin", "ContractAdmin123", options))
			if err != nil {
				t.Fatal(err)
			}
			defer admin.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			for _, q := range []string{`CREATE USER CKAN_APP IDENTIFIED BY "ContractApp123"`, `GRANT DWROLE TO CKAN_APP`, `ALTER USER CKAN_APP QUOTA UNLIMITED ON DATA`} {
				if _, err = admin.DB.ExecContext(ctx, q); err != nil {
					t.Fatal(err)
				}
			}
			store, err := Open(go_ora.BuildUrl("localhost", port, service, "CKAN_APP", "ContractApp123", options))
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if err = store.Migrate(ctx); err != nil {
				t.Fatal(err)
			}
			if err = store.Migrate(ctx); err != nil {
				t.Fatal(err)
			}
			repo := &OracleSourceResourceCatalogRepository{DB: store.DB, JobsTopic: "jobs", ResultsTopic: "results", SyncResultsTopic: "sync-results"}
			contracttest.Run(t, store, repo)
			if t.Failed() {
				return
			}
			for _, count := range []int{20448, 200000} {
				sourceID := uuid.NewString()
				now := time.Now().UTC()
				source := r.Instance{ID: sourceID, Name: "load-" + sourceID, URL: "https://test", SourceType: "sicom", CreatedAt: now, UpdatedAt: now}
				if err = store.CreateInstance(context.Background(), &source); err != nil {
					t.Fatal(err)
				}
				values := make([]r.Resource, count)
				for i := range values {
					values[i] = r.Resource{ID: r.ResourceID(sourceID, strconv.Itoa(i)), SourceID: sourceID, Kind: "file", Title: fmt.Sprintf("SICOM.2026.%07d.licitacao.csv", 3100000+i), URL: "https://test/" + strconv.Itoa(i) + ".csv", Version: "109826", Format: "CSV", DatasetName: "Licitações"}
				}
				for _, phase := range []string{"insert", "same_sync_replay", "new_sync_replay"} {
					phaseCtx, phaseCancel := context.WithTimeout(context.Background(), 120*time.Second)
					syncID := sourceID
					if phase == "new_sync_replay" {
						syncID = uuid.NewString()
					}
					before := time.Now()
					for i := 0; i < count; i += 200 {
						if err = repo.ObserveBatch(phaseCtx, syncID, values[i:min(i+200, count)]); err != nil {
							break
						}
					}
					phaseCancel()
					t.Logf("memory=%s rows=%d phase=%s seconds=%.3f stats=%s", limit, count, phase, time.Since(before).Seconds(), docker(t, "stats", "--no-stream", "--format", "{{json .}}", name))
					if err != nil {
						t.Logf("workload exceeded memory/time budget: %v", err)
						return
					}
				}
			}
			viable = true
			t.Logf("minimum_validated_memory=%s startup_seconds=%.3f", limit, time.Since(started).Seconds())
		})
		if t.Failed() {
			t.Fatal("Oracle contract/configuration failure requires a fix before advancing memory")
		}
		if viable {
			return
		}
	}
	t.Fatal("Autonomous Free failed the sizing criteria at every tested limit")
}
func docker(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v: %s", args[0], err, out)
	}
	return string(out)
}
