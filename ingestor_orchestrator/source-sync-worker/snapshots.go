// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package sourcesyncworker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	catalog "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/source-resource-catalog"
)

type snapshotClient interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

type SnapshotStore struct {
	Client snapshotClient
	Bucket string
}

func newSnapshotStore() *SnapshotStore {
	endpoint := env("S3_ENDPOINT", "rustfs:9000")
	if !strings.Contains(endpoint, "://") {
		scheme := "http://"
		if env("S3_USE_SSL", "false") == "true" {
			scheme = "https://"
		}
		endpoint = scheme + endpoint
	}
	client := s3.New(s3.Options{
		Region: env("S3_REGION", "auto"), BaseEndpoint: aws.String(endpoint),
		UsePathStyle:               env("S3_URL_STYLE", "path") == "path",
		Credentials:                credentials.NewStaticCredentialsProvider(os.Getenv("S3_ACCESS_KEY_ID"), os.Getenv("S3_SECRET_ACCESS_KEY"), ""),
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	})
	return &SnapshotStore{Client: client, Bucket: env("S3_BUCKET", "public-datasets")}
}

func (s *SnapshotStore) Publish(ctx context.Context, syncID string, source catalog.Source, at time.Time, records io.ReadSeeker) error {
	file, err := os.CreateTemp("", "catalog-upload-*.jsonl")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	header := map[string]any{"version": 1, "source": source, "sync_id": syncID, "collected_at": at.Format(time.RFC3339Nano)}
	if err = json.NewEncoder(file).Encode(header); err != nil {
		return err
	}
	if _, err = records.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err = io.Copy(file, records); err != nil {
		return err
	}
	size, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	_, err = s.Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.Bucket), Key: aws.String("catalog-inbox/" + url.PathEscape(source.ID) + "/" + url.PathEscape(syncID) + ".jsonl"),
		Body: file, ContentLength: aws.Int64(size), ContentType: aws.String("application/x-ndjson"), IfNoneMatch: aws.String("*"),
	})
	var api smithy.APIError
	if errors.As(err, &api) && api.ErrorCode() == "PreconditionFailed" {
		return nil
	}
	return err
}
