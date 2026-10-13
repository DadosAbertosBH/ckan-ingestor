// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// SPDX-License-Identifier: AGPL-3.0-or-later
// Licensed under the GNU Affero General Public License, version 3 or later.
package crawler

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

type memoryStorage struct {
	objects map[string][]byte
	headErr error
	failPut bool
}

func (s *memoryStorage) Head(_ context.Context, k string) error {
	if s.headErr != nil {
		return s.headErr
	}
	if _, ok := s.objects[k]; !ok {
		return ErrNotFound
	}
	return nil
}
func (s *memoryStorage) Get(_ context.Context, k string) (io.ReadCloser, error) {
	b, ok := s.objects[k]
	if !ok {
		return nil, ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}
func (s *memoryStorage) Put(_ context.Context, k string, b io.ReadSeeker, _ int64, _ string) error {
	if s.failPut {
		return errors.New("upload failed")
	}
	v, e := io.ReadAll(b)
	s.objects[k] = v
	return e
}
func (s *memoryStorage) URL(k string) string { return "https://files.test/" + k }

type fakePortal struct {
	bytes     []byte
	downloads int
	years     []int
	listErr   map[int]error
}

func (p *fakePortal) Years(context.Context) ([]int, error) { return p.years, nil }
func (p *fakePortal) List(_ context.Context, y int) ([]Category, error) {
	if e := p.listErr[y]; e != nil {
		return nil, e
	}
	return []Category{{SeqZip: int64(y), Name: "Despesas com pessoal", Year: y}}, nil
}
func (p *fakePortal) Download(context.Context, Category) (io.ReadCloser, error) {
	p.downloads++
	return io.NopCloser(bytes.NewReader(p.bytes)), nil
}
func (p *fakePortal) DownloadURL(c Category) string {
	return "https://portal.test/zip/" + stringID(c.SeqZip)
}

type fakePublisher struct {
	jobs []Job
	fail bool
}

func (p *fakePublisher) Publish(_ context.Context, j Job) error {
	if p.fail {
		return errors.New("broker down")
	}
	p.jobs = append(p.jobs, j)
	return nil
}
func zipped(t *testing.T, m map[string][]byte) []byte {
	t.Helper()
	var b bytes.Buffer
	z := zip.NewWriter(&b)
	for n, v := range m {
		w, e := z.Create(n)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = w.Write(v); e != nil {
			t.Fatal(e)
		}
	}
	if e := z.Close(); e != nil {
		t.Fatal(e)
	}
	return b.Bytes()
}
func stringID(n int64) string { return strconv.FormatInt(n, 10) }
func fixture(t *testing.T) (*Runner, *fakePortal, *memoryStorage, *fakePublisher) {
	p := &fakePortal{bytes: zipped(t, map[string][]byte{"municipio.zip": zipped(t, map[string][]byte{"pessoal.csv": []byte("id;valor\n1;20\n")}), "ignored.exe": []byte("x")}), years: []int{2026}}
	s := &memoryStorage{objects: map[string][]byte{}}
	q := &fakePublisher{}
	return &Runner{Portal: p, Storage: s, Publisher: q, InstanceID: "instance", TempDir: t.TempDir()}, p, s, q
}

func TestCategoryCompleteLogIncludesCategoryName(t *testing.T) {
	r, _, _, _ := fixture(t)
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	if err := r.publish(context.Background(), Category{Year: 2026, SeqZip: 109823, Name: "Despesas com pessoal"}, nil); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "category=\"Despesas com pessoal\"") {
		t.Fatalf("log does not include category name: %s", logs.String())
	}
}

func TestCategoryLifecycleLogsIncludeCategoryIdentity(t *testing.T) {
	r, _, _, _ := fixture(t)
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, message := range []string{"category started", "download started", "download completed", "processing started", "category complete"} {
		expected := "msg=\"" + message + "\" year=2026 category=\"Despesas com pessoal\" seq_zip=2026"
		if !strings.Contains(logs.String(), expected) {
			t.Fatalf("log does not include %q identity: %s", message, logs.String())
		}
	}
}

func TestNestedZIPCacheAndDailyReplay(t *testing.T) {
	r, p, s, q := fixture(t)
	ctx := context.Background()
	if e := r.Run(ctx); e != nil {
		t.Fatal(e)
	}
	if len(q.jobs) != 1 || q.jobs[0].ResourceName != "municipio.zip!/pessoal.csv" || q.jobs[0].ResourceFormat != "CSV" {
		t.Fatalf("jobs=%+v", q.jobs)
	}
	first := q.jobs[0]
	if !strings.HasPrefix(first.ResourceURL, "https://files.test/crawled-files/") || first.DatastoreActive {
		t.Fatalf("job=%+v", first)
	}
	if len(s.objects) != 3 {
		t.Fatalf("objects=%d", len(s.objects))
	}
	if e := r.Run(ctx); e != nil {
		t.Fatal(e)
	}
	if p.downloads != 1 || !reflect.DeepEqual(first, q.jobs[1]) {
		t.Fatalf("downloads=%d jobs=%+v", p.downloads, q.jobs)
	}
}

func TestLocalSICOMArchive(t *testing.T) {
	filename := os.Getenv("SICOM_TEST_ZIP")
	if filename == "" {
		t.Skip("set SICOM_TEST_ZIP to validate an already downloaded archive with in-memory storage/broker")
	}
	body, err := os.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	r, p, _, q := fixture(t)
	p.bytes = body
	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(q.jobs) == 0 {
		t.Fatal("archive has no supported members")
	}
	count := len(q.jobs)
	t.Logf("materialized archive members: %d", count)
	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p.downloads != 1 || len(q.jobs) != 2*count {
		t.Fatalf("cache replay: downloads=%d jobs=%d", p.downloads, len(q.jobs))
	}
}
func TestRebuildManifestFromCachedZIP(t *testing.T) {
	r, p, s, q := fixture(t)
	if e := r.Run(context.Background()); e != nil {
		t.Fatal(e)
	}
	for k := range s.objects {
		if strings.HasPrefix(k, "crawled-discovery/") {
			delete(s.objects, k)
		}
	}
	if e := r.Run(context.Background()); e != nil {
		t.Fatal(e)
	}
	if p.downloads != 1 || len(q.jobs) != 2 {
		t.Fatalf("downloads=%d jobs=%d", p.downloads, len(q.jobs))
	}
}
func TestStorageErrorsNeverTriggerOriginDownload(t *testing.T) {
	r, p, s, q := fixture(t)
	s.headErr = errors.New("access denied")
	if e := r.Run(context.Background()); e == nil {
		t.Fatal("expected storage failure")
	}
	if p.downloads != 0 || len(q.jobs) != 0 {
		t.Fatal("storage failure triggered work")
	}
}
func TestFailedUploadPublishesNothing(t *testing.T) {
	r, _, s, q := fixture(t)
	s.failPut = true
	if e := r.Run(context.Background()); e == nil {
		t.Fatal("expected upload failure")
	}
	if len(q.jobs) != 0 {
		t.Fatal("published before uploads")
	}
}
func TestFailedPublicationResumesFromCache(t *testing.T) {
	r, p, _, q := fixture(t)
	q.fail = true
	if e := r.Run(context.Background()); e == nil {
		t.Fatal("expected publication failure")
	}
	q.fail = false
	if e := r.Run(context.Background()); e != nil {
		t.Fatal(e)
	}
	if p.downloads != 1 || len(q.jobs) != 1 {
		t.Fatal("failed to resume")
	}
}
func TestListingFailureContinuesOtherYears(t *testing.T) {
	r, p, _, q := fixture(t)
	p.years = []int{2025, 2026}
	p.listErr = map[int]error{2025: errors.New("origin unavailable")}
	if e := r.Run(context.Background()); e == nil {
		t.Fatal("expected partial failure")
	}
	if len(q.jobs) != 1 {
		t.Fatalf("jobs=%d", len(q.jobs))
	}
}
func TestRejectsUnsafeAndOversizedArchives(t *testing.T) {
	for _, tc := range []struct {
		name   string
		files  map[string][]byte
		limits Limits
	}{
		{"traversal", map[string][]byte{"../bad.csv": []byte("x")}, Limits{}},
		{"expansion", map[string][]byte{"big.csv": []byte("12345")}, Limits{ExpandedBytes: 4}},
		{"download", map[string][]byte{"a.csv": []byte("x")}, Limits{ZIPBytes: 4}},
		{"depth", map[string][]byte{"a.zip": zipped(t, map[string][]byte{"b.zip": zipped(t, map[string][]byte{"a.csv": []byte("x")})})}, Limits{Depth: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, p, _, q := fixture(t)
			p.bytes = zipped(t, tc.files)
			r.Limits = tc.limits
			if e := r.Run(context.Background()); e == nil {
				t.Fatal("expected archive rejection")
			}
			if len(q.jobs) != 0 {
				t.Fatal("published invalid archive")
			}
		})
	}
}
