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

use crate::job_repository::JobRepository;
use crate::metadata_processor::ResourceCandidate;

#[derive(Debug, Clone)]
pub struct JobPlan {
    pub candidate: ResourceCandidate,
    pub enqueue: bool,
    pub retry: bool,
}

pub struct JobPlanner {
    repository: Box<dyn JobRepository>,
}

impl JobPlanner {
    pub fn new(repository: Box<dyn JobRepository>) -> Self {
        Self { repository }
    }

    pub fn classify(&self, candidates: Vec<ResourceCandidate>) -> Result<Vec<JobPlan>> {
        let in_flight = self.repository.in_flight_resource_ids()?;
        let failed = self.repository.failed_resource_ids()?;
        Ok(classify_candidates(candidates, &in_flight, &failed))
    }
}

pub fn classify_candidates(
    candidates: Vec<ResourceCandidate>,
    in_flight: &HashSet<String>,
    failed: &HashSet<String>,
) -> Vec<JobPlan> {
    candidates
        .into_iter()
        .map(|candidate| {
            let enqueue = !in_flight.contains(&candidate.resource_id);
            let retry = enqueue && failed.contains(&candidate.resource_id);
            JobPlan {
                candidate,
                enqueue,
                retry,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockJobRepository {
        in_flight: HashSet<String>,
        failed: HashSet<String>,
    }

    impl JobRepository for MockJobRepository {
        fn in_flight_resource_ids(&self) -> Result<HashSet<String>> {
            Ok(self.in_flight.clone())
        }

        fn failed_resource_ids(&self) -> Result<HashSet<String>> {
            Ok(self.failed.clone())
        }
    }

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
        let planner = JobPlanner::new(Box::new(MockJobRepository {
            in_flight: HashSet::from(["in-flight".to_string()]),
            failed: HashSet::from(["failed".to_string()]),
        }));
        let planned = planner
            .classify(vec![
                candidate("new"),
                candidate("failed"),
                candidate("in-flight"),
            ])
            .unwrap();
        assert_eq!(
            planned
                .iter()
                .map(|plan| (
                    plan.candidate.resource_id.as_str(),
                    plan.enqueue,
                    plan.retry
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
        let planner = JobPlanner::new(Box::new(MockJobRepository {
            in_flight: HashSet::new(),
            failed: HashSet::new(),
        }));
        let planned = planner.classify(vec![candidate(id)]).unwrap();
        assert_eq!(planned[0].candidate.resource_id, id);
        assert!(planner.classify(vec![]).unwrap().is_empty());
    }
}
