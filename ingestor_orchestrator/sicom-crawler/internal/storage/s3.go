// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package storage

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-crawler/internal/crawler"
	"io"
	"net/url"
	"strings"
)

type Store struct {
	Client            *s3.Client
	Bucket, PublicURL string
}

func New(ctx context.Context, endpoint, bucket, publicURL, key, secret, region string, pathStyle bool) (*Store, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region), config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(key, secret, "")))
	if err != nil {
		return nil, err
	}
	cli := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = pathStyle
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
	return &Store{Client: cli, Bucket: bucket, PublicURL: strings.TrimRight(publicURL, "/")}, nil
}
func translate(err error) error {
	var api smithy.APIError
	if errors.As(err, &api) && (api.ErrorCode() == "NotFound" || api.ErrorCode() == "NoSuchKey") {
		return crawler.ErrNotFound
	}
	return err
}
func (s *Store) Head(ctx context.Context, key string) error {
	_, err := s.Client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.Bucket), Key: aws.String(key)})
	return translate(err)
}
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	res, err := s.Client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.Bucket), Key: aws.String(key)})
	if err != nil {
		return nil, translate(err)
	}
	return res.Body, nil
}
func (s *Store) Put(ctx context.Context, key string, body io.ReadSeeker, size int64, contentType string) error {
	// Seekable temporary files keep large transfers off the heap. Failed multipart uploads are aborted by the SDK.
	uploader := manager.NewUploader(s.Client, func(u *manager.Uploader) { u.PartSize = 16 << 20; u.Concurrency = 2 })
	_, err := uploader.Upload(ctx, &s3.PutObjectInput{Bucket: aws.String(s.Bucket), Key: aws.String(key), Body: body, ContentLength: aws.Int64(size), ContentType: aws.String(contentType)})
	return err
}
func (s *Store) URL(key string) string {
	parts := strings.Split(key, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return s.PublicURL + "/" + strings.Join(parts, "/")
}
