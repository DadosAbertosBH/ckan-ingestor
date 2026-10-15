// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// SPDX-License-Identifier: AGPL-3.0-or-later
// Licensed under the GNU Affero General Public License, version 3 or later.
package crawler

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"strconv"
	"strings"

	jobmessage "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/ingestor-job-message-contracts"
	sicomidentity "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-source-identity"
)

var ErrNotFound = errors.New("object not found")

type Category struct {
	SeqZip int64  `json:"seqZip"`
	Name   string `json:"categoria"`
	Year   int    `json:"-"`
}
type Portal interface {
	Years(context.Context) ([]int, error)
	List(context.Context, int) ([]Category, error)
	Download(context.Context, Category) (io.ReadCloser, error)
	DownloadURL(Category) string
}
type Storage interface {
	Head(context.Context, string) error
	Get(context.Context, string) (io.ReadCloser, error)
	Put(context.Context, string, io.ReadSeeker, int64, string) error
	URL(string) string
}
type Job = jobmessage.WorkerJob
type Publisher interface {
	Publish(context.Context, Job) error
}
type Limits struct {
	ZIPBytes, ExpandedBytes int64
	Depth, Entries          int
}
type Runner struct {
	Portal     Portal
	Storage    Storage
	Publisher  Publisher
	InstanceID string
	TempDir    string
	Formats    []string
	Limits     Limits
}

type manifest struct {
	Version int    `json:"version"`
	Origin  string `json:"origin"`
	Jobs    []Job  `json:"jobs"`
}

func (r *Runner) Run(ctx context.Context) error {
	years, err := r.Portal.Years(ctx)
	if err != nil {
		return fmt.Errorf("discover years: %w", err)
	}
	var failures []error
	for _, year := range years {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(failures, err)...)
		}
		categories, err := r.Portal.List(ctx, year)
		if err != nil {
			failures = append(failures, fmt.Errorf("list %d: %w", year, err))
			continue
		}
		for _, c := range categories {
			c.Year = year
			if err := r.process(ctx, c); err != nil {
				failures = append(failures, fmt.Errorf("year %d ZIP %d: %w", year, c.SeqZip, err))
				slog.Error("category failed", "year", year, "seq_zip", c.SeqZip)
			}
		}
	}
	return errors.Join(failures...)
}
func (r *Runner) limits() Limits {
	l := r.Limits
	if l.ZIPBytes == 0 {
		l.ZIPBytes = 8 << 30
	}
	if l.ExpandedBytes == 0 {
		l.ExpandedBytes = 64 << 30
	}
	if l.Depth == 0 {
		l.Depth = 4
	}
	if l.Entries == 0 {
		l.Entries = 100000
	}
	return l
}
func (r *Runner) process(ctx context.Context, c Category) error {
	origin := r.Portal.DownloadURL(c)
	archiveKey := "crawled-files/" + sicomidentity.StableID(origin) + "/archive.zip"
	manifestKey := "crawled-discovery/sicom/" + sicomidentity.StableID(origin) + "/members.json"
	// HEAD the immutable original first: a missing manifest never authorizes an origin download.
	cached := true
	if err := r.Storage.Head(ctx, archiveKey); errors.Is(err, ErrNotFound) {
		cached = false
	} else if err != nil {
		return err
	}
	if cached {
		body, err := r.Storage.Get(ctx, manifestKey)
		if err == nil {
			var m manifest
			decodeErr := json.NewDecoder(io.LimitReader(body, 64<<20)).Decode(&m)
			_ = body.Close()
			if decodeErr == nil && m.Version == 1 && m.Origin == origin {
				complete := true
				for _, j := range m.Jobs {
					// Validate persisted identities before using a manifest from storage.
					expected := r.job(c, j.ResourceName, j.ResourceFormat)
					if j != expected {
						complete = false
						break
					}
					if err := r.Storage.Head(ctx, r.memberKey(origin, j.ResourceName)); errors.Is(err, ErrNotFound) {
						complete = false
						break
					} else if err != nil {
						return err
					}
				}
				if complete {
					slog.Info("manifest found, skipping processing", "year", c.Year, "category", c.Name, "seq_zip", c.SeqZip)
					slog.Info("processing started", "year", c.Year, "category", c.Name, "seq_zip", c.SeqZip)
					return r.publish(ctx, c, m.Jobs)
				}
			}
		} else if !errors.Is(err, ErrNotFound) {
			return err
		}
	}
	var body io.ReadCloser
	var err error
	if cached {
		slog.Info("download zip cache", "year", c.Year, "category", c.Name, "seq_zip", c.SeqZip)
		body, err = r.Storage.Get(ctx, archiveKey)
	} else {
		slog.Info("download started", "year", c.Year, "category", c.Name, "seq_zip", c.SeqZip)
		body, err = r.Portal.Download(ctx, c)
	}
	if err != nil {
		return err
	}
	tmp, err := r.spool(body, r.limits().ZIPBytes)
	_ = body.Close()
	if err != nil {
		return err
	}
	if !cached {
		slog.Info("download completed", "year", c.Year, "category", c.Name, "seq_zip", c.SeqZip)
	}
	defer removeTemp(tmp)
	info, err := tmp.Stat()
	if err != nil {
		return err
	}
	slog.Info("processing started", "year", c.Year, "category", c.Name, "seq_zip", c.SeqZip)
	z, err := zip.NewReader(tmp, info.Size())
	if err != nil {
		return fmt.Errorf("invalid ZIP: %w", err)
	}
	// Reading every member validates CRCs and safety before persisting the source ZIP.
	if err := r.validate(ctx, z, 0, &archiveBudget{}); err != nil {
		return err
	}
	if !cached {
		if err := r.Storage.Put(ctx, archiveKey, tmp, info.Size(), "application/zip"); err != nil {
			return err
		}
	}
	jobs := []Job{}
	if err := r.extract(ctx, c, z, "", 0, &archiveBudget{}, &jobs); err != nil {
		return err
	}
	encoded, err := json.Marshal(manifest{Version: 1, Origin: origin, Jobs: jobs})
	if err != nil {
		return err
	}
	if err := r.Storage.Put(ctx, manifestKey, bytes.NewReader(encoded), int64(len(encoded)), "application/json"); err != nil {
		return err
	}
	return r.publish(ctx, c, jobs)
}
func (r *Runner) publish(ctx context.Context, c Category, jobs []Job) error {
	for _, j := range jobs {
		if err := r.Publisher.Publish(ctx, j); err != nil {
			return err
		}
	}
	slog.Info("category complete", "year", c.Year, "category", c.Name, "seq_zip", c.SeqZip, "jobs", len(jobs))
	return nil
}
func (r *Runner) memberKey(origin, name string) string {
	return "crawled-files/" + sicomidentity.StableID(origin+"!/"+name) + "/" + path.Base(name)
}
func (r *Runner) job(c Category, name, format string) Job {
	origin := r.Portal.DownloadURL(c)
	resourceID := sicomidentity.StableID(origin + "!/" + name)
	version := strconv.FormatInt(c.SeqZip, 10)
	return Job{JobID: sicomidentity.StableID(resourceID + ":" + version), ResourceID: resourceID, PackageID: sicomidentity.StableID(sicomidentity.PortalURL + "SICOM/" + strconv.Itoa(c.Year) + "/" + c.Name), CKANURL: sicomidentity.PortalURL, InstanceID: r.InstanceID, DatasetName: fmt.Sprintf("SICOM %d — %s", c.Year, c.Name), ResourceName: name, ResourceURL: r.Storage.URL(r.memberKey(origin, name)), ResourceFormat: format, SourceVersion: version}
}
func (r *Runner) spool(src io.Reader, limit int64) (*os.File, error) {
	f, err := os.CreateTemp(r.TempDir, "sicom-*")
	if err != nil {
		return nil, err
	}
	n, err := io.Copy(f, io.LimitReader(src, limit+1))
	if err == nil && n > limit {
		err = errors.New("archive byte limit exceeded")
	}
	if err == nil {
		_, err = f.Seek(0, io.SeekStart)
	}
	if err != nil {
		removeTemp(f)
		return nil, err
	}
	return f, nil
}
func removeTemp(f *os.File) { name := f.Name(); _ = f.Close(); _ = os.Remove(name) }

type archiveBudget struct {
	bytes   int64
	entries int
}

func safeName(name string) bool {
	if name == "" || strings.ContainsAny(name, "\\\x00!") || strings.HasPrefix(name, "/") || strings.Contains(name, ":") {
		return false
	}
	for _, part := range strings.Split(name, "/") {
		if part == ".." {
			return false
		}
	}
	return path.Clean(strings.TrimSuffix(name, "/")) == strings.TrimSuffix(name, "/")
}
func (r *Runner) walk(ctx context.Context, c Category, z *zip.Reader, prefix string, depth int, budget *archiveBudget, jobs *[]Job) error {
	if depth > r.limits().Depth {
		return errors.New("archive nesting limit exceeded")
	}
	seen := map[string]bool{}
	for _, entry := range z.File {
		if err := ctx.Err(); err != nil {
			return err
		}
		budget.entries++
		if budget.entries > r.limits().Entries {
			return errors.New("archive entry limit exceeded")
		}
		if !safeName(entry.Name) || seen[entry.Name] || entry.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("unsafe or duplicate archive member %q", entry.Name)
		}
		seen[entry.Name] = true
		if entry.FileInfo().IsDir() {
			continue
		}
		remaining := r.limits().ExpandedBytes - budget.bytes
		if remaining < 0 || entry.UncompressedSize64 > uint64(remaining) {
			return errors.New("archive expansion limit exceeded")
		}
		reader, err := entry.Open()
		if err != nil {
			return err
		}
		tmp, err := r.spool(reader, remaining)
		_ = reader.Close()
		if err != nil {
			return err
		}
		info, err := tmp.Stat()
		if err != nil {
			removeTemp(tmp)
			return err
		}
		budget.bytes += info.Size()
		member := prefix + entry.Name
		format := strings.ToUpper(strings.TrimPrefix(path.Ext(entry.Name), "."))
		if format == "ZIP" {
			nested, e := zip.NewReader(tmp, info.Size())
			if e == nil {
				e = r.walk(ctx, c, nested, member+"!/", depth+1, budget, jobs)
			}
			removeTemp(tmp)
			if e != nil {
				return e
			}
		} else {
			if jobs != nil && r.supports(format) {
				key := r.memberKey(r.Portal.DownloadURL(c), member)
				if err = r.Storage.Head(ctx, key); errors.Is(err, ErrNotFound) {
					err = r.Storage.Put(ctx, key, tmp, info.Size(), contentType(format))
				}
				if err != nil {
					removeTemp(tmp)
					return err
				}
				*jobs = append(*jobs, r.job(c, member, format))
			}
			removeTemp(tmp)
		}
	}
	return nil
}
func (r *Runner) validate(ctx context.Context, z *zip.Reader, depth int, b *archiveBudget) error {
	return r.walk(ctx, Category{}, z, "", depth, b, nil)
}
func (r *Runner) extract(ctx context.Context, c Category, z *zip.Reader, p string, d int, b *archiveBudget, j *[]Job) error {
	return r.walk(ctx, c, z, p, d, b, j)
}
func (r *Runner) supports(format string) bool {
	formats := r.Formats
	if len(formats) == 0 {
		formats = []string{"CSV", "JSON", "GEOJSON"}
	}
	for _, f := range formats {
		if strings.EqualFold(f, format) {
			return true
		}
	}
	return false
}
func contentType(format string) string {
	switch format {
	case "CSV":
		return "text/csv"
	case "JSON":
		return "application/json"
	case "GEOJSON":
		return "application/geo+json"
	default:
		return "application/octet-stream"
	}
}
