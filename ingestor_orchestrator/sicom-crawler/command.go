// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package sicomcrawler

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	iggytransport "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/iggy-message-transport"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-crawler/internal/broker"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-crawler/internal/config"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-crawler/internal/crawler"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-crawler/internal/portal"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-crawler/internal/storage"
	sicomidentity "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-source-identity"
	catalog "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/source-resource-catalog"
)

func Main() {
	discover := flag.Bool("discover-only", false, "Authenticate and list ZIPs; read-only, without R2 or queue access")
	year := flag.Int("year", 0, "Restrict to one published year (default: all)")
	category := flag.String("category", "", "Restrict to a category name (case-insensitive)")
	interactive := flag.Bool("interactive", false, "Allow up to 30 minutes for CAPTCHA intervention in an external visual CDP browser")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, *discover, *interactive, *year, *category); err != nil {
		slog.Error("crawler failed", "error", err)
		os.Exit(1)
	}
}
func run(ctx context.Context, discover, interactive bool, year int, category string) error {
	return runObserved(ctx, discover, interactive, year, category, "", nil)
}
func runObserved(ctx context.Context, discover, interactive bool, year int, category string, sourceID string, emit func(catalog.Resource) error) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if !discover && (cfg.AccessKey == "" || cfg.SecretKey == "" || (emit == nil && cfg.IggyPassword == "")) {
		return errors.New("configure S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY and IGGY_PASSWORD")
	}
	client, pem, err := portal.HTTPClient(cfg.CAFile)
	if err != nil {
		return err
	}
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return errors.New("too many redirects")
		}
		if len(via) > 0 && req.URL.Host != via[0].URL.Host {
			req.Header.Del("Authorization")
			req.Header.Del("AuthorizationProxy")
		}
		return nil
	}
	endpoint := cfg.CDPURL
	if endpoint == "" {
		var closeBrowser func()
		endpoint, closeBrowser, err = startBrowser(ctx, cfg.Lightpanda, pem)
		if err != nil {
			return err
		}
		defer closeBrowser()
	}
	authTimeout := 2 * time.Minute
	if interactive {
		authTimeout = 30 * time.Minute
	}
	browser := &portal.Browser{CDPURL: endpoint, HTTP: client, Timeout: authTimeout, Interactive: interactive}
	api := &portal.Client{HTTP: client, Authenticate: browser.Authenticate}
	source := &filteredPortal{Client: api, year: year, category: category}
	if emit != nil {
		cfg.InstanceID = sourceID
		source.observe = func(c crawler.Category, y int) error {
			key := "category:" + c.Name
			if err := emit(catalog.Resource{ID: catalog.ResourceID(sourceID, key), Kind: "collection", Title: c.Name, DatasetName: c.Name, PageURL: sicomidentity.DownloadPageURL}); err != nil {
				return err
			}
			return emit(catalog.Resource{ID: catalog.ResourceID(sourceID, key+"/"+fmt.Sprint(y)), ParentID: catalog.ResourceID(sourceID, key), Kind: "collection", Title: fmt.Sprint(y), DatasetName: c.Name, PageURL: sicomidentity.DownloadPageURL, Version: fmt.Sprint(c.SeqZip)})
		}
	}
	if discover {
		years, err := source.Years(ctx)
		if err != nil {
			return err
		}
		out := json.NewEncoder(os.Stdout)
		for _, year := range years {
			categories, err := source.List(ctx, year)
			if err != nil {
				return err
			}
			for _, c := range categories {
				if err := out.Encode(map[string]any{"year": year, "category": c.Name, "seq_zip": c.SeqZip, "download_url": source.DownloadURL(c)}); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if emit == nil {
		if err := validateInstance(ctx, client, cfg.OrchestratorURL, cfg.InstanceID); err != nil {
			return err
		}
	}
	store, err := storage.New(ctx, cfg.Endpoint, cfg.Bucket, cfg.PublicURL, cfg.AccessKey, cfg.SecretKey, cfg.Region, cfg.PathStyle)
	if err != nil {
		return err
	}
	if emit != nil {
		runner := &crawler.Runner{Portal: source, Storage: store, Publisher: observationPublisher{SourceID: sourceID, Emit: emit}, InstanceID: sourceID, Formats: cfg.Formats, Limits: crawler.Limits{ZIPBytes: cfg.ZIPBytes, ExpandedBytes: cfg.ExpandedBytes, Depth: cfg.Depth, Entries: cfg.Entries}}
		return runner.Run(ctx)
	}
	queue, err := iggytransport.Connect(ctx, iggytransport.ConnectionConfig{Address: cfg.IggyAddress, Username: cfg.IggyUsername, Password: cfg.IggyPassword, Stream: cfg.Stream})
	if err != nil {
		return err
	}
	defer queue.Close()
	runner := &crawler.Runner{Portal: source, Storage: store, Publisher: &broker.Publisher{Send: func(ctx context.Context, topic, key string, payload []byte) error {
		_, err := queue.Send(ctx, topic, key, payload)
		return err
	}, JobsTopic: cfg.JobsTopic, ResultsTopic: cfg.ResultsTopic}, InstanceID: cfg.InstanceID, Formats: cfg.Formats, Limits: crawler.Limits{ZIPBytes: cfg.ZIPBytes, ExpandedBytes: cfg.ExpandedBytes, Depth: cfg.Depth, Entries: cfg.Entries}}
	return runner.Run(ctx)
}

type filteredPortal struct {
	*portal.Client
	year     int
	category string
	observe  func(crawler.Category, int) error
}

func (p *filteredPortal) Years(ctx context.Context) ([]int, error) {
	years, err := p.Client.Years(ctx)
	if err != nil {
		return nil, err
	}
	if p.year == 0 {
		return years, nil
	}
	for _, y := range years {
		if y == p.year {
			return []int{y}, nil
		}
	}
	return nil, fmt.Errorf("year %d is not published by SICOM", p.year)
}
func (p *filteredPortal) List(ctx context.Context, year int) ([]crawler.Category, error) {
	list, err := p.Client.List(ctx, year)
	if err != nil {
		return nil, err
	}
	if p.category == "" {
		if p.observe != nil {
			for _, c := range list {
				if err := p.observe(c, year); err != nil {
					return nil, err
				}
			}
		}
		return list, nil
	}
	filtered := []crawler.Category{}
	for _, c := range list {
		if strings.EqualFold(c.Name, p.category) {
			filtered = append(filtered, c)
		}
	}
	if len(filtered) == 0 {
		return nil, fmt.Errorf("selected category absent in year %d", year)
	}
	return filtered, nil
}
func validateInstance(ctx context.Context, client *http.Client, base, id string) error {
	req, err := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(base, "/")+"/api/instances/"+id, nil)
	if err != nil {
		return err
	}
	response, err := client.Do(req)
	if err != nil {
		return errors.New("orchestrator instance preflight failed")
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return fmt.Errorf("SICOM instance preflight HTTP %d; apply orchestrator migrations first", response.StatusCode)
	}
	var instance struct {
		ID, URL    string
		SourceType string `json:"source_type"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&instance); err != nil {
		return errors.New("invalid orchestrator instance response")
	}
	if instance.ID != id || instance.URL != sicomidentity.PortalURL || instance.SourceType != sicomidentity.SourceType {
		return errors.New("configured instance must be the TCE-MG SICOM source")
	}
	return nil
}
func startBrowser(ctx context.Context, binary string, pem []byte) (string, func(), error) {
	cert, err := os.CreateTemp("", "sicom-ca-*.pem")
	if err != nil {
		return "", nil, err
	}
	certPath := cert.Name()
	if _, err := cert.Write(pem); err != nil {
		_ = cert.Close()
		_ = os.Remove(certPath)
		return "", nil, err
	}
	_ = cert.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = os.Remove(certPath)
		return "", nil, err
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	owned, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(owned, binary, "serve", "--host", "127.0.0.1", "--port", fmt.Sprint(port), "--ca-cert", certPath, "--load-resources", "iframe", "--load-resources", "worker", "--log-level", "error")
	// Browser errors can contain JWT-bearing redirect URLs. Keep child output private.
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	cmd.Env = append(os.Environ(), "LIGHTPANDA_DISABLE_TELEMETRY=true", "LIGHTPANDA_DISABLE_CORE_DUMP=true")
	if err := cmd.Start(); err != nil {
		cancel()
		_ = os.Remove(certPath)
		return "", nil, errors.New("start Lightpanda failed; configure SICOM_LIGHTPANDA_BIN or SICOM_CDP_URL")
	}
	exited := make(chan struct{})
	go func() { _ = cmd.Wait(); close(exited) }()
	closeBrowser := func() { cancel(); <-exited; _ = os.Remove(certPath) }
	address := fmt.Sprintf("http://127.0.0.1:%d", port)
	probe := &http.Client{Timeout: time.Second}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(15 * time.Second)
	defer timeout.Stop()
	for {
		select {
		case <-ctx.Done():
			closeBrowser()
			return "", nil, ctx.Err()
		case <-exited:
			closeBrowser()
			return "", nil, errors.New("Lightpanda exited during startup")
		case <-timeout.C:
			closeBrowser()
			return "", nil, errors.New("Lightpanda CDP startup timed out")
		case <-ticker.C:
			response, e := probe.Get(address + "/json/version")
			if e == nil {
				_ = response.Body.Close()
				if response.StatusCode == 200 {
					return address, closeBrowser, nil
				}
			}
		}
	}
}
