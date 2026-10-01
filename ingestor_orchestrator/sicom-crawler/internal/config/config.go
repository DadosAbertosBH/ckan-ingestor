// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package config

import (
	"fmt"
	sicomidentity "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-source-identity"
	"net/url"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Endpoint, Bucket, PublicURL, AccessKey, SecretKey, Region                string
	PathStyle                                                                bool
	IggyAddress, IggyUsername, IggyPassword, Stream, JobsTopic, ResultsTopic string
	CDPURL, Lightpanda, OrchestratorURL, InstanceID, CAFile                  string
	Formats                                                                  []string
	ZIPBytes, ExpandedBytes                                                  int64
	Depth, Entries                                                           int
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
func Load() (Config, error) {
	c := Config{Endpoint: env("S3_ENDPOINT", "rustfs:9000"), Bucket: env("S3_BUCKET", "public-datasets"), PublicURL: os.Getenv("S3_PUBLIC_URL"), AccessKey: os.Getenv("S3_ACCESS_KEY_ID"), SecretKey: os.Getenv("S3_SECRET_ACCESS_KEY"), Region: env("S3_REGION", "auto"), PathStyle: env("S3_URL_STYLE", "path") == "path", IggyAddress: env("IGGY_ADDRESS", "iggy:8090"), IggyUsername: env("IGGY_USERNAME", "iggy"), IggyPassword: os.Getenv("IGGY_PASSWORD"), Stream: env("IGGY_STREAM", "ckan-ingestor"), JobsTopic: env("IGGY_TOPIC", "jobs"), ResultsTopic: env("IGGY_TOPIC_RESULTS", "job-results"), CDPURL: os.Getenv("SICOM_CDP_URL"), Lightpanda: env("SICOM_LIGHTPANDA_BIN", "lightpanda"), OrchestratorURL: env("SICOM_ORCHESTRATOR_URL", "http://localhost:8000"), InstanceID: env("SICOM_INSTANCE_ID", sicomidentity.InstanceID()), CAFile: os.Getenv("SICOM_CA_BUNDLE"), Formats: strings.Split(env("SICOM_FORMATS", "CSV,JSON,GEOJSON"), ",")}
	if !strings.Contains(c.Endpoint, "://") {
		scheme := "http://"
		if env("S3_USE_SSL", "false") == "true" {
			scheme = "https://"
		}
		c.Endpoint = scheme + c.Endpoint
	}
	c.Endpoint = strings.TrimRight(c.Endpoint, "/")
	if c.PublicURL == "" {
		c.PublicURL = c.Endpoint + "/" + c.Bucket
	}
	c.PublicURL = strings.TrimRight(c.PublicURL, "/")
	for _, address := range []string{c.Endpoint, c.PublicURL, c.OrchestratorURL} {
		u, e := url.Parse(address)
		if e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
			return Config{}, fmt.Errorf("invalid service URL configuration")
		}
	}
	for _, setting := range []struct {
		key      string
		target   *int64
		fallback int64
	}{{"SICOM_MAX_ZIP_BYTES", &c.ZIPBytes, 8 << 30}, {"SICOM_MAX_EXPANDED_BYTES", &c.ExpandedBytes, 64 << 30}} {
		n, e := strconv.ParseInt(env(setting.key, strconv.FormatInt(setting.fallback, 10)), 10, 64)
		if e != nil || n <= 0 || n == int64(^uint64(0)>>1) {
			return Config{}, fmt.Errorf("%s must be a positive byte limit", setting.key)
		}
		*setting.target = n
	}
	for _, setting := range []struct {
		key      string
		target   *int
		fallback int
	}{{"SICOM_MAX_ARCHIVE_DEPTH", &c.Depth, 4}, {"SICOM_MAX_ARCHIVE_ENTRIES", &c.Entries, 100000}} {
		n, e := strconv.Atoi(env(setting.key, strconv.Itoa(setting.fallback)))
		if e != nil || n <= 0 {
			return Config{}, fmt.Errorf("%s must be positive", setting.key)
		}
		*setting.target = n
	}
	for i, f := range c.Formats {
		c.Formats[i] = strings.ToUpper(strings.TrimSpace(f))
		if c.Formats[i] == "" {
			return Config{}, fmt.Errorf("SICOM_FORMATS contains an empty format")
		}
	}
	return c, nil
}
