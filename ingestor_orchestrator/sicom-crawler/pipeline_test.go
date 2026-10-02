// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package sicomcrawler

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-crawler/internal/broker"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-crawler/internal/crawler"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-crawler/internal/portal"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-crawler/internal/storage"
	sicomidentity "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-source-identity"
)

func archive(t *testing.T, name string, body []byte) []byte {
	t.Helper()
	var b bytes.Buffer
	writer := zip.NewWriter(&b)
	entry, e := writer.Create(name)
	if e != nil {
		t.Fatal(e)
	}
	if _, e := entry.Write(body); e != nil {
		t.Fatal(e)
	}
	if e := writer.Close(); e != nil {
		t.Fatal(e)
	}
	return b.Bytes()
}
func TestHTTPPipelineCachesNestedZIPAndRepublishesCompatibleJobs(t *testing.T) {
	payload := archive(t, "municipio.zip", archive(t, "despPessoal.csv", []byte("id;valor\n1;20\n")))
	downloads := 0
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("AuthorizationProxy") != "token session" {
			t.Error("portal request missing authentication")
		}
		switch r.URL.Path {
		case "/buscarCategoriaDownload":
			fmt.Fprint(w, `[{"seqZip":109822,"categoria":"Despesas com pessoal"}]`)
		case "/baixarArquivoPct/109822":
			downloads++
			_, _ = w.Write(payload)
		default:
			w.WriteHeader(404)
		}
	}))
	defer api.Close()
	mu := sync.Mutex{}
	objects := map[string][]byte{}
	r2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			t.Error("R2 API requires signed requests")
		}
		mu.Lock()
		defer mu.Unlock()
		key := strings.TrimPrefix(r.URL.Path, "/bucket/")
		if r.Method == "PUT" {
			data, e := io.ReadAll(r.Body)
			if e != nil {
				t.Error(e)
			}
			objects[key] = data
			w.Header().Set("ETag", `"etag"`)
			return
		}
		if data, ok := objects[key]; ok {
			if r.Method != "HEAD" {
				_, _ = w.Write(data)
			}
		} else {
			w.WriteHeader(404)
		}
	}))
	defer r2.Close()
	ctx := context.Background()
	store, e := storage.New(ctx, r2.URL, "bucket", r2.URL+"/bucket", "key", "secret", "auto", true)
	if e != nil {
		t.Fatal(e)
	}
	source := &portal.Client{HTTP: api.Client(), BaseURL: api.URL + "/", Authenticate: func(context.Context) (portal.Session, error) {
		return portal.Session{ProxyToken: "session", Bearer: "app", Years: []int{2026}}, nil
	}}
	topics := []string{}
	messages := []crawler.Job{}
	publisher := &broker.Publisher{JobsTopic: "jobs", ResultsTopic: "job-results", Send: func(_ context.Context, topic, key string, data []byte) error {
		topics = append(topics, topic)
		var job crawler.Job
		if e := json.Unmarshal(data, &job); e != nil {
			t.Fatal(e)
		}
		if job.InstanceID != sicomidentity.InstanceID() || job.CKANURL != sicomidentity.PortalURL || job.ResourceFormat != "CSV" || job.SourceVersion != "109822" {
			t.Errorf("incompatible job: %+v", job)
		}
		if topic == "jobs" {
			messages = append(messages, job)
		}
		return nil
	}}
	runner := &crawler.Runner{Portal: source, Storage: store, Publisher: publisher, InstanceID: sicomidentity.InstanceID(), TempDir: t.TempDir()}
	for i := 0; i < 2; i++ {
		if e := runner.Run(ctx); e != nil {
			t.Fatal(e)
		}
	}
	if downloads != 1 || len(objects) != 3 || len(messages) != 2 || !reflect.DeepEqual(messages[0], messages[1]) {
		t.Fatalf("downloads=%d objects=%d jobs=%+v", downloads, len(objects), messages)
	}
	if !reflect.DeepEqual(topics, []string{"job-results", "jobs", "job-results", "jobs"}) {
		t.Fatalf("topics=%v", topics)
	}
}
