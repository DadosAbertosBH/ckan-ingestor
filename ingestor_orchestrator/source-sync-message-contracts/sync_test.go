// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package sourcesync

import "testing"

func TestLegacyCommandDefaultsToCKAN(t *testing.T) {
	c, err := Decode([]byte(`{"sync_id":"sync","instance_id":"source","instance_url":"https://example.test"}`))
	if err != nil || c.SourceType != "ckan" {
		t.Fatalf("command=%+v error=%v", c, err)
	}
}
func TestRejectsIncompleteAndUnknownSourceCommands(t *testing.T) {
	for _, body := range []string{`{}`, `{"sync_id":"s","instance_id":"i","source_type":"unknown"}`} {
		if _, err := Decode([]byte(body)); err == nil {
			t.Fatalf("accepted %s", body)
		}
	}
}
