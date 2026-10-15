// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// SPDX-License-Identifier: AGPL-3.0-or-later
// Licensed under the GNU Affero General Public License, version 3 or later.
package app

import repositories "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/repository-contracts"

type JobStatus = repositories.JobStatus
type Job = repositories.Job
type JobResult = repositories.JobResult
type LatestResource = repositories.LatestResource
type TerminalState = repositories.TerminalState
type Instance = repositories.Instance
type MetadataSync = repositories.MetadataSync
type JobResultMessage = repositories.JobResultMessage
type MetadataSyncResultMessage = repositories.MetadataSyncResultMessage
type Routing = repositories.Routing
type JobQuery = repositories.JobQuery
type ResourceQuery = repositories.ResourceQuery
type SyncQuery = repositories.SyncQuery
type JobView = repositories.JobView
type ResourceView = repositories.ResourceView
type DatasetView = repositories.DatasetView
type InstanceStats = repositories.InstanceStats

const (
	JobPending    = repositories.JobPending
	JobProcessing = repositories.JobProcessing
	JobCompleted  = repositories.JobCompleted
	JobFailed     = repositories.JobFailed
	JobDeleted    = repositories.JobDeleted
)
