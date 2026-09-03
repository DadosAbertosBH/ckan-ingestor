// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use std::collections::HashSet;

use anyhow::Result;
use mysql::prelude::Queryable;
use mysql::{OptsBuilder, Pool};

use crate::metadata_processor::ResourceCandidate;

pub struct MysqlJobPlanner {
    pool: Pool,
}

impl MysqlJobPlanner {
    pub fn from_pool(pool: Pool) -> Self {
        Self { pool }
    }

    pub fn from_env() -> Result<Self> {
        let host = std::env::var("INGEST_ORCH_MYSQL_HOST").unwrap_or_else(|_| "localhost".into());
        let port = std::env::var("INGEST_ORCH_MYSQL_PORT")
            .unwrap_or_else(|_| "3306".into())
            .parse()?;
        let user = std::env::var("INGEST_ORCH_MYSQL_USER").unwrap_or_else(|_| "root".into());
        let password = std::env::var("INGEST_ORCH_MYSQL_PASSWORD").unwrap_or_default();
        let database = std::env::var("INGEST_ORCH_MYSQL_DATABASE")
            .unwrap_or_else(|_| "ingestor_orchestrator".into());
        let options = OptsBuilder::new()
            .ip_or_hostname(Some(host))
            .tcp_port(port)
            .user(Some(user))
            .pass(Some(password))
            .db_name(Some(database));
        Ok(Self::from_pool(Pool::new(options)?))
    }

    pub fn classify(
        &self,
        candidates: Vec<ResourceCandidate>,
    ) -> Result<Vec<(ResourceCandidate, bool, bool)>> {
        let mut conn = self.pool.get_conn()?;
        let in_flight: HashSet<String> = conn.query_map(
            "SELECT idempotency_key FROM ckan_data_job WHERE status IN ('pending', 'processing')",
            |id| id,
        )?.into_iter().collect();
        let failed: HashSet<String> = conn.query_map("SELECT c.resource_id FROM ckan_data_job c JOIN latest_resource_job l ON l.latest_job_id = c.id WHERE c.status = 'failed'", |id| id)?.into_iter().collect();
        Ok(classify_candidates(candidates, &in_flight, &failed))
    }
}

pub fn classify_candidates(
    candidates: Vec<ResourceCandidate>,
    in_flight: &HashSet<String>,
    failed: &HashSet<String>,
) -> Vec<(ResourceCandidate, bool, bool)> {
    candidates
        .into_iter()
        .map(|candidate| {
            let enqueue = !in_flight.contains(&candidate.resource_id);
            let retry = enqueue && failed.contains(&candidate.resource_id);
            (candidate, enqueue, retry)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(id: &str) -> ResourceCandidate {
        ResourceCandidate {
            resource_id: id.into(),
            resource_name: None,
            resource_url: None,
            resource_format: None,
            dataset_name: "dataset".into(),
            datastore_active: false,
        }
    }

    #[test]
    fn classifies_new_failed_and_in_flight_outdated_resources() {
        let in_flight = HashSet::from(["in-flight".to_string()]);
        let failed = HashSet::from(["failed".to_string()]);
        let planned = classify_candidates(
            vec![
                candidate("new"),
                candidate("failed"),
                candidate("in-flight"),
            ],
            &in_flight,
            &failed,
        );
        assert_eq!(
            planned
                .iter()
                .map(|(candidate, enqueue, retry)| (
                    candidate.resource_id.as_str(),
                    *enqueue,
                    *retry
                ))
                .collect::<Vec<_>>(),
            vec![
                ("new", true, false),
                ("failed", true, true),
                ("in-flight", false, false)
            ]
        );
    }

    #[test]
    fn preserves_uuid_like_resource_ids_and_empty_input() {
        let id = "a6b97d48-a9fb-4991-9893-d920ffb19b90";
        let planned = classify_candidates(vec![candidate(id)], &HashSet::new(), &HashSet::new());
        assert_eq!(planned[0].0.resource_id, id);
        assert!(classify_candidates(vec![], &HashSet::new(), &HashSet::new()).is_empty());
    }
}
