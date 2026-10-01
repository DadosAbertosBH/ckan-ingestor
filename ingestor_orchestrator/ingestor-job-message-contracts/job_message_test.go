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

package jobmessage

import (
	"encoding/json"
	"testing"
)

func TestPendingCarriesTheWorkerJobIdentityAndMetadata(t *testing.T) {
	job := WorkerJob{JobID: "job", ResourceID: "resource", DatasetName: "SICOM 2026", InstanceID: "sicom", CKANURL: "https://dadosabertos.tce.mg.gov.br/", ResourceURL: "https://files.test/pessoal.csv", ResourceFormat: "CSV", SourceVersion: "109822"}
	pending := Pending(job)
	if pending.Status != "PENDING" || pending.JobID != job.JobID || pending.ResourceID != job.ResourceID || pending.DatasetName == nil || *pending.DatasetName != job.DatasetName || pending.InstanceID == nil || *pending.InstanceID != job.InstanceID || pending.DatastoreActive == nil || *pending.DatastoreActive {
		t.Fatalf("pending=%+v", pending)
	}
	encoded, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	var decoded WorkerJob
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded != job {
		t.Fatalf("decoded=%+v", decoded)
	}
}
