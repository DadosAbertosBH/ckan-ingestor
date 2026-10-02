// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package sourcesync

import (
	"encoding/json"
	"errors"
)

type Command struct {
	SyncID     string `json:"sync_id"`
	SourceID   string `json:"instance_id"`
	SourceName string `json:"instance_name"`
	SourceURL  string `json:"instance_url"`
	SourceType string `json:"source_type,omitempty"`
}

func Decode(data []byte) (Command, error) {
	var c Command
	if err := json.Unmarshal(data, &c); err != nil {
		return c, err
	}
	if c.SourceType == "" {
		c.SourceType = "ckan"
	}
	if c.SyncID == "" || c.SourceID == "" || (c.SourceType != "ckan" && c.SourceType != "sicom") {
		return c, errors.New("invalid source sync command")
	}
	return c, nil
}
