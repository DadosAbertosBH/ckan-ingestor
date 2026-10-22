// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package sourcesyncworker

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	catalog "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/source-resource-catalog"
)

type snapshotUpload struct {
	key       string
	content   []byte
	condition string
	err       error
}

func (u *snapshotUpload) PutObject(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	u.key, u.condition = *input.Key, *input.IfNoneMatch
	u.content, _ = io.ReadAll(input.Body)
	return &s3.PutObjectOutput{}, u.err
}
func TestSnapshotUploadIsAnImmutableAtomicJSONLArtifact(t *testing.T) {
	upload := &snapshotUpload{}
	store := SnapshotStore{Client: upload, Bucket: "lake"}
	err := store.Publish(context.Background(), "sync", catalog.Source{ID: "source", Name: "Portal", Type: "ckan"}, time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC), strings.NewReader("{\"id\":\"r\"}\n"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(upload.content)), "\n")
	if len(lines) != 2 || upload.key != "catalog-inbox/source/sync.jsonl" || upload.condition != "*" {
		t.Fatalf("upload=%+v", upload)
	}
	var header map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &header); err != nil {
		t.Fatal(err)
	}
	if header["sync_id"] != "sync" || header["version"] != float64(1) {
		t.Fatalf("header=%s", lines[0])
	}
	upload.err = &smithy.GenericAPIError{Code: "PreconditionFailed"}
	if err := store.Publish(context.Background(), "sync", catalog.Source{ID: "source"}, time.Now(), strings.NewReader("")); err != nil {
		t.Fatal(err)
	}
}
