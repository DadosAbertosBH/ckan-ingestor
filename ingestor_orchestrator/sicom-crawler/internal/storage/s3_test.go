// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-crawler/internal/crawler"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestS3SignedStreamingAndNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			t.Error("unsigned request")
		}
		switch r.URL.Path {
		case "/bucket/missing":
			w.WriteHeader(404)
		case "/bucket/forbidden":
			w.WriteHeader(403)
		case "/bucket/member.csv":
			if r.Method == "PUT" {
				b, e := io.ReadAll(r.Body)
				if e != nil || string(b) != "id\n1\n" {
					t.Errorf("body=%q err=%v", b, e)
				}
				w.Header().Set("ETag", `"etag"`)
			} else {
				fmt.Fprint(w, "id\n1\n")
			}
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(400)
		}
	}))
	defer server.Close()
	cli := s3.New(s3.Options{Region: "auto", Credentials: credentials.NewStaticCredentialsProvider("key", "secret", ""), BaseEndpoint: aws.String(server.URL), UsePathStyle: true, RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired, ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired})
	store := &Store{Client: cli, Bucket: "bucket", PublicURL: "https://files.test"}
	ctx := context.Background()
	if e := store.Head(ctx, "missing"); !errors.Is(e, crawler.ErrNotFound) {
		t.Fatalf("missing=%v", e)
	}
	if e := store.Head(ctx, "forbidden"); e == nil || errors.Is(e, crawler.ErrNotFound) {
		t.Fatalf("403=%v", e)
	}
	if e := store.Put(ctx, "member.csv", bytes.NewReader([]byte("id\n1\n")), 5, "text/csv"); e != nil {
		t.Fatal(e)
	}
	body, e := store.Get(ctx, "member.csv")
	if e != nil {
		t.Fatal(e)
	}
	defer body.Close()
	b, e := io.ReadAll(body)
	if e != nil || string(b) != "id\n1\n" {
		t.Fatalf("download=%q error=%v", b, e)
	}
	if u := store.URL("folder/a b.csv"); u != "https://files.test/folder/a%20b.csv" {
		t.Fatalf("URL=%s", u)
	}
}
