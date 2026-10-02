// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package sicomcrawler

import (
	jobmessage "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/ingestor-job-message-contracts"
	"testing"
)

func TestMemberIdentitySeparatesExerciseFromPublication(t *testing.T) {
	j := jobmessage.WorkerJob{DatasetName: "SICOM 2026 — Despesas com pessoal", ResourceName: "municipio.zip!/pessoal.csv", SourceVersion: "109822", ResourceURL: "https://r2.test/member.csv", ResourceID: "legacy"}
	first, err := MemberResource("source", j)
	if err != nil {
		t.Fatal(err)
	}
	j.SourceVersion = "109999"
	second, err := MemberResource("source", j)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID || first.Version == second.Version {
		t.Fatal("publication changed logical member identity")
	}
	if first.DatasetName != "Despesas com pessoal" || first.ID != "legacy" {
		t.Fatalf("resource=%+v", first)
	}
	j.DatasetName = "SICOM 2025 — Despesas com pessoal"
	j.ResourceID = "member-from-2025"
	third, _ := MemberResource("source", j)
	if third.ID == first.ID || third.ParentID == first.ParentID {
		t.Fatal("exercise is missing from identity")
	}
}
