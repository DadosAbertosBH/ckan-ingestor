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

// Package sicomidentity defines the canonical identity and public URLs of TCE-MG SICOM.
package sicomidentity

import (
	"net/url"

	"github.com/google/uuid"
)

const PortalURL = "https://dadosabertos.tce.mg.gov.br/"
const DownloadPageURL = "https://dadosabertos.tce.mg.gov.br/#/public/detalhar-categoria/jurisdicionado/download"
const SourceType = "sicom"

func StableID(identity string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(identity)).String()
}
func InstanceID() string { return StableID(PortalURL) }
func IsPortalURL(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil && parsed.Hostname() == "dadosabertos.tce.mg.gov.br"
}
