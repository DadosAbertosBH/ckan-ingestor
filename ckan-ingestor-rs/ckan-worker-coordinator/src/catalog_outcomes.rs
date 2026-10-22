// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.

use anyhow::Result;
use ckan_ingestor_worker_lib::JobResultMessage;
use object_store::{PutMode, PutOptions, parse_url_opts};
use std::time::{SystemTime, UNIX_EPOCH};
use url::Url;

pub async fn persist_outcome(
    root: &str,
    options: &[(String, String)],
    result: &JobResultMessage,
) -> Result<()> {
    let Some(source) = result.instance_id.as_deref().filter(|id| !id.is_empty()) else {
        return Ok(());
    };
    let source_key = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, source.as_bytes());
    let status = serde_json::to_string(&result.status)?;
    let event_key = uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_URL,
        format!("{}:{status}", result.job_id).as_bytes(),
    );
    let relative = format!("catalog-results/{source_key}/{event_key}.json");
    let uri = if root.contains("://") {
        Url::parse(&format!("{}/{relative}", root.trim_end_matches('/')))?
    } else {
        Url::from_file_path(std::path::Path::new(root).join(relative))
            .map_err(|_| anyhow::anyhow!("absolute data path required"))?
    };
    let (store, path) = parse_url_opts(&uri, options.iter().cloned())?;
    // Keep only catalog state here; previews and ingestion errors remain in the operational ledger.
    let record = serde_json::json!({
        "instance_id": source, "resource_id": result.resource_id, "job_id": result.job_id,
        "source_version": result.source_version, "status": result.status,
        "recorded_at": SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros() as u64,
    });
    match store
        .put_opts(
            &path,
            serde_json::to_vec(&record)?.into(),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await
    {
        Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ckan_ingestor_worker_lib::JobStatus;

    #[tokio::test]
    async fn outcomes_preserve_identity_status_and_are_immutable_on_replay() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().to_str().unwrap();
        let mut result = JobResultMessage::pending("job", "resource", "Dataset", "source");
        result.status = JobStatus::Failed;
        result.source_version = Some("v2".into());
        persist_outcome(root, &[], &result).await.unwrap();
        let source = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, b"source");
        let directory = temp.path().join("catalog-results").join(source.to_string());
        assert!(directory.exists(), "terminal result was not persisted");
        let path = std::fs::read_dir(&directory)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let before = std::fs::read(&path).unwrap();
        persist_outcome(root, &[], &result).await.unwrap();
        assert_eq!(before, std::fs::read(path).unwrap());
        let document: serde_json::Value = serde_json::from_slice(&before).unwrap();
        assert_eq!(document["instance_id"], "source");
        assert_eq!(document["resource_id"], "resource");
        assert_eq!(document["source_version"], "v2");
        assert_eq!(document["status"], "FAILED");
        assert!(document["recorded_at"].as_u64().is_some());
    }
}
