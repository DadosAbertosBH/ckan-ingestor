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

use crate::job_repository::JobRepository;

pub struct MySqlJobRepository {
    pool: Pool,
}

impl MySqlJobRepository {
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
}

impl JobRepository for MySqlJobRepository {
    fn in_flight_resource_ids(&self) -> Result<HashSet<String>> {
        let mut conn = self.pool.get_conn()?;
        Ok(conn
            .query_map(
                "SELECT idempotency_key FROM ckan_data_job WHERE status IN ('pending', 'processing')",
                |id| id,
            )?
            .into_iter()
            .collect())
    }

    fn failed_resource_ids(&self) -> Result<HashSet<String>> {
        let mut conn = self.pool.get_conn()?;
        Ok(conn
            .query_map(
                "SELECT c.resource_id FROM ckan_data_job c JOIN latest_resource_job l ON l.latest_job_id = c.id WHERE c.status = 'failed'",
                |id| id,
            )?
            .into_iter()
            .collect())
    }
}
