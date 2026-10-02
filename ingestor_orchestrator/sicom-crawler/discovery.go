// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package sicomcrawler

import (
	"context"
	"fmt"
	jobmessage "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/ingestor-job-message-contracts"
	catalog "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/source-resource-catalog"
	"strconv"
	"strings"
)

type Adapter struct{}

func (a Adapter) Discover(ctx context.Context, s catalog.Source, emit func(catalog.Resource) error) error {
	return runObserved(ctx, false, false, 0, "", s.ID, emit)
}

type observationPublisher struct {
	SourceID string
	Emit     func(catalog.Resource) error
}

func (p observationPublisher) Publish(ctx context.Context, j jobmessage.WorkerJob) error {
	r, err := MemberResource(p.SourceID, j)
	if err != nil {
		return err
	}
	return p.Emit(r)
}
func MemberResource(source string, j jobmessage.WorkerJob) (catalog.Resource, error) {
	parts := strings.SplitN(j.DatasetName, " — ", 2)
	if len(parts) != 2 {
		return catalog.Resource{}, fmt.Errorf("invalid SICOM dataset name")
	}
	year, err := strconv.Atoi(strings.TrimPrefix(parts[0], "SICOM "))
	if err != nil {
		return catalog.Resource{}, err
	}
	parent := "category:" + parts[1] + "/" + strconv.Itoa(year)
	return catalog.Resource{ID: j.ResourceID, ParentID: catalog.ResourceID(source, parent), Kind: "file", Title: j.ResourceName, URL: j.ResourceURL, Format: j.ResourceFormat, Version: j.SourceVersion, DatasetName: parts[1]}, nil
}
