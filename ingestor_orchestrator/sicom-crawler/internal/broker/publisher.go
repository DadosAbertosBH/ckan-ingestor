// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package broker

import (
	"context"
	"encoding/json"
	"fmt"
	jobmessage "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/ingestor-job-message-contracts"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-crawler/internal/crawler"
)

type Send func(context.Context, string, string, []byte) error
type Publisher struct {
	Send                    Send
	JobsTopic, ResultsTopic string
}

func (p *Publisher) Publish(ctx context.Context, j crawler.Job) error {
	payload, err := json.Marshal(jobmessage.Pending(j))
	if err != nil {
		return err
	}
	if err := p.Send(ctx, p.ResultsTopic, j.ResourceID, payload); err != nil {
		return fmt.Errorf("publish PENDING: %w", err)
	}
	payload, err = json.Marshal(j)
	if err != nil {
		return err
	}
	if err := p.Send(ctx, p.JobsTopic, j.ResourceID, payload); err != nil {
		return fmt.Errorf("publish worker job: %w", err)
	}
	return nil
}
