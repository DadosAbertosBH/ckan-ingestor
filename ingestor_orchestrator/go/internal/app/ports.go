// Noctcloud Desenvolvimento LTDA
// Copyright (C) 2026  Noctcloud Desenvolvimento LTDA
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
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package app

import "context"

type Tx interface {
	Job(context.Context, string) (*Job, error)
	InsertJob(context.Context, *Job) error
	UpdateJob(context.Context, *Job) error
	InsertResult(context.Context, *JobResult) error
	Latest(context.Context, string) (*LatestResource, error)
	PutLatest(context.Context, *LatestResource) error
	Terminal(context.Context, string) (*TerminalState, error)
	PutTerminal(context.Context, *TerminalState) error
	AddLabel(context.Context, string, string) error
	RemoveLabel(context.Context, string, string) error
	PutCSVHint(context.Context, string, string) error
	CSVHint(context.Context, string) (*string, error)
	Sync(context.Context, string) (*MetadataSync, error)
	InsertSync(context.Context, *MetadataSync) error
	UpdateSync(context.Context, *MetadataSync) error
	Instance(context.Context, string) (*Instance, error)
	UpdateInstance(context.Context, *Instance) error
}

type Store interface {
	InTx(context.Context, func(Tx) error) error
	Instances(context.Context) ([]Instance, error)
	Ping(context.Context) error
}

type Publisher interface {
	Publish(context.Context, string, []byte, string) (Routing, error)
}
