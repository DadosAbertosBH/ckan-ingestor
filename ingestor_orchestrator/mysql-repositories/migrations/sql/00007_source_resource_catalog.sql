-- +goose Up
CREATE TABLE source_resource (
 id CHAR(36) NOT NULL,
 source_id CHAR(36) NOT NULL,
 parent_resource_id CHAR(36) NULL,
 kind VARCHAR(16) NOT NULL,
 title TEXT NOT NULL,
 description TEXT NOT NULL,
 page_url TEXT NOT NULL,
 content_url TEXT NOT NULL,
 format VARCHAR(50) NOT NULL,
 source_version TEXT NOT NULL,
 dataset_name TEXT NOT NULL,
 datastore_active BOOLEAN NOT NULL DEFAULT FALSE,
 first_seen_sync CHAR(36) NOT NULL,
 last_changed_sync CHAR(36) NOT NULL,
 last_seen_sync CHAR(36) NOT NULL,
 deleted_at DATETIME NULL,
 created_at DATETIME NOT NULL,
 updated_at DATETIME NOT NULL,
 PRIMARY KEY(source_id,id),
 KEY ix_source_resource_source(source_id,deleted_at),
 KEY ix_source_resource_parent(source_id,parent_resource_id),
 FOREIGN KEY(source_id) REFERENCES ckan_instance(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE source_resource_version (
 id CHAR(36) PRIMARY KEY,
 resource_id CHAR(36) NOT NULL,
 source_id CHAR(36) NOT NULL,
 source_version TEXT NOT NULL,
 content_url TEXT NOT NULL,
 job_id CHAR(36) NOT NULL,
 status VARCHAR(16) NOT NULL DEFAULT 'discovered',
 created_at DATETIME NOT NULL,
 updated_at DATETIME NOT NULL,
 KEY ix_source_resource_version_job(job_id),
 FOREIGN KEY(source_id,resource_id) REFERENCES source_resource(source_id,id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE source_sync_outbox (
 sequence_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
 id CHAR(36) NOT NULL UNIQUE,
 topic VARCHAR(255) NOT NULL,
 routing_key CHAR(36) NOT NULL,
 payload JSON NOT NULL,
 delivered_at DATETIME NULL,
 created_at DATETIME NOT NULL,
 KEY ix_source_sync_outbox_pending(delivered_at,sequence_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE source_sync_run (
 sync_id CHAR(36) PRIMARY KEY,
 source_id CHAR(36) NOT NULL,
 status VARCHAR(16) NOT NULL,
 counts JSON NOT NULL,
 completed_at DATETIME NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- +goose Down
DROP TABLE source_sync_run;
DROP TABLE source_sync_outbox;
DROP TABLE source_resource_version;
DROP TABLE source_resource;
