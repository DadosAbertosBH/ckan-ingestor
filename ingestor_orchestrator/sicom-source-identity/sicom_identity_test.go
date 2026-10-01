// Noctcloud Desenvolvimento LTDA
// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package sicomidentity

import "testing"

func TestCanonicalSICOMIdentityAndURLsRemainStable(t *testing.T) {
	if SourceType != "sicom" {
		t.Fatalf("source type = %q", SourceType)
	}
	if PortalURL != "https://dadosabertos.tce.mg.gov.br/" {
		t.Fatalf("portal URL = %q", PortalURL)
	}
	if DownloadPageURL != "https://dadosabertos.tce.mg.gov.br/#/public/detalhar-categoria/jurisdicionado/download" {
		t.Fatalf("download URL = %q", DownloadPageURL)
	}
	if InstanceID() != "4be9dd37-1f28-540d-a7c3-104e4f5a54e8" {
		t.Fatalf("instance ID = %s", InstanceID())
	}
	if StableID(PortalURL) != InstanceID() {
		t.Fatal("instance ID must derive from the canonical portal URL")
	}
	if !IsPortalURL("https://dadosabertos.tce.mg.gov.br/") || !IsPortalURL("https://dadosabertos.tce.mg.gov.br/path") || IsPortalURL("https://dadosabertos.tce.mg.gov.br.example/") {
		t.Fatal("portal URL recognition is incorrect")
	}
}
