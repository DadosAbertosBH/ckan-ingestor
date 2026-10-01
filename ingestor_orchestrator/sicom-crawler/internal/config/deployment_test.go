// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package config

import (
	"os"
	"strings"
	"testing"
)

func TestImagePinsCorrectedLightpandaAndBuildsCrawler(t *testing.T) {
	data, err := os.ReadFile("../../Dockerfile")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"7e890133b78fa2127096e7464a3ee6977cc93d36", "github.com/guitcastro/browser.git", "linux/arm64", "./cmd/sicom-crawler", "tini", "USER 10001"} {
		if !strings.Contains(string(data), required) {
			t.Errorf("Dockerfile missing %s", required)
		}
	}
}
