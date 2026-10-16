// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package config

import (
	"os"
	"strings"
	"testing"
)

func TestImageUsesOfficialLightpandaNightlyAndBuildsCrawler(t *testing.T) {
	data, err := os.ReadFile("../../Dockerfile")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"FROM lightpanda/browser:nightly AS lightpanda", "COPY --from=lightpanda /bin/lightpanda /usr/local/bin/lightpanda", "COPY --from=go-builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt", "ARG TARGETARCH", "./cmd/sicom-crawler", "ENTRYPOINT [\"/usr/local/bin/sicom-crawler\"]", "USER 10001"} {
		if !strings.Contains(string(data), required) {
			t.Errorf("Dockerfile missing %s", required)
		}
	}
	if strings.Contains(string(data), "apt-get") || strings.Contains(string(data), "tini") {
		t.Error("Dockerfile must use the builder CA bundle without apt or tini")
	}
	dependencyCopy := "COPY ingestor_orchestrator/repository-contracts ./repository-contracts"
	if strings.Index(string(data), dependencyCopy) == -1 || strings.Index(string(data), dependencyCopy) > strings.Index(string(data), "RUN go mod download") {
		t.Error("Dockerfile must copy every locally replaced module before go mod download")
	}
}
