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
	for _, required := range []string{"FROM lightpanda/browser:nightly AS lightpanda", "COPY --from=lightpanda /bin/lightpanda /usr/local/bin/lightpanda", "ARG TARGETARCH", "./cmd/sicom-crawler", "tini", "USER 10001"} {
		if !strings.Contains(string(data), required) {
			t.Errorf("Dockerfile missing %s", required)
		}
	}
}
