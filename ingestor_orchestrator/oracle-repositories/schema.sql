CREATE TABLE ckan_instance (
 id VARCHAR2(36 CHAR) NOT NULL PRIMARY KEY,
 name VARCHAR2(255 CHAR) NOT NULL UNIQUE,
 url VARCHAR2(512 CHAR) NOT NULL,
 last_metadata_synced TIMESTAMP(6) NULL,
 dataset_count NUMBER(19) DEFAULT 0 NOT NULL,
 resource_count NUMBER(19) DEFAULT 0 NOT NULL,
 created_at TIMESTAMP(6) NOT NULL,
 updated_at TIMESTAMP(6) NOT NULL,
 source_type VARCHAR2(16 CHAR) DEFAULT 'ckan' NOT NULL
);

CREATE TABLE ckan_data_job (
 id VARCHAR2(36 CHAR) NOT NULL PRIMARY KEY,
 resource_id VARCHAR2(255 CHAR) NOT NULL,
 resource_name VARCHAR2(512 CHAR),
 resource_url CLOB,
 resource_format VARCHAR2(50 CHAR),
 status VARCHAR2(16 CHAR) NOT NULL,
 idempotency_key VARCHAR2(255 CHAR) NOT NULL,
 created_at TIMESTAMP(6) NOT NULL,
 updated_at TIMESTAMP(6) NOT NULL,
 started_at TIMESTAMP(6),
 completed_at TIMESTAMP(6),
 dataset_name VARCHAR2(512 CHAR),
 instance_id VARCHAR2(36 CHAR) NOT NULL,
 ckan_url VARCHAR2(512 CHAR) DEFAULT '',
 message_topic VARCHAR2(255 CHAR),
 message_partition NUMBER(19),
 message_offset NUMBER(19),
 datastore_active NUMBER(1) DEFAULT 0 NOT NULL,
 broker_type VARCHAR2(32 CHAR),
 message_stream VARCHAR2(255 CHAR),
 CONSTRAINT fk_ckan_data_job_instance_id FOREIGN KEY(instance_id) REFERENCES ckan_instance(id),
 parent_job_id VARCHAR2(36 CHAR) NULL,
 parent_resource_id VARCHAR2(255 CHAR) NULL,
 child_index NUMBER(19) NULL,
 total_child_count NUMBER(19) NULL,
 source_version VARCHAR2(255 CHAR) NULL,
 CONSTRAINT fk_ckan_data_job_parent FOREIGN KEY(parent_job_id) REFERENCES ckan_data_job(id),
 CONSTRAINT ck_job_status CHECK(status IN ('pending','processing','completed','failed','deleted','outdated'))
);

CREATE TABLE ckan_data_job_result (
 id VARCHAR2(36 CHAR) NOT NULL PRIMARY KEY,
 job_id VARCHAR2(36 CHAR) NOT NULL,
 success NUMBER(1) NOT NULL,
 error_message CLOB,
 error_trace CLOB,
 dataset_preview CLOB,
 rows_processed NUMBER(19),
 created_at TIMESTAMP(6) NOT NULL,
 expected_rows NUMBER(19),
 resource_size NUMBER(19),
 encoding VARCHAR2(50 CHAR),
 status VARCHAR2(16 CHAR) NOT NULL,
 FOREIGN KEY(job_id) REFERENCES ckan_data_job(id) ON DELETE CASCADE,
 CONSTRAINT ck_result_status CHECK(status IN ('pending','processing','completed','failed','deleted'))
);

CREATE TABLE csv_hint (
 resource_id VARCHAR2(255 CHAR) NOT NULL PRIMARY KEY,
 delimiter VARCHAR2(10 CHAR) NOT NULL
);

CREATE TABLE latest_resource_job (
 resource_id VARCHAR2(255 CHAR) NOT NULL PRIMARY KEY,
 latest_job_id VARCHAR2(36 CHAR) NOT NULL,
 instance_id VARCHAR2(36 CHAR) NOT NULL,
 resource_name VARCHAR2(512 CHAR),
 resource_url CLOB,
 resource_format VARCHAR2(50 CHAR),
 dataset_name VARCHAR2(512 CHAR),
 status VARCHAR2(16 CHAR) NOT NULL,
 created_at TIMESTAMP(6) NOT NULL,
 updated_at TIMESTAMP(6) NOT NULL,
 FOREIGN KEY(latest_job_id) REFERENCES ckan_data_job(id),
 FOREIGN KEY(instance_id) REFERENCES ckan_instance(id)
);

CREATE TABLE last_terminal_status (
 resource_id VARCHAR2(255 CHAR) NOT NULL PRIMARY KEY,
 last_terminal_job_id VARCHAR2(36 CHAR) NOT NULL,
 last_terminal_status VARCHAR2(16 CHAR) NOT NULL,
 last_terminal_at TIMESTAMP(6) NOT NULL,
 last_successful_job_id VARCHAR2(36 CHAR),
 FOREIGN KEY(last_terminal_job_id) REFERENCES ckan_data_job(id),
 FOREIGN KEY(last_successful_job_id) REFERENCES ckan_data_job(id)
);

CREATE TABLE metadata_sync (
 id VARCHAR2(36 CHAR) NOT NULL PRIMARY KEY,
 instance_id VARCHAR2(36 CHAR) NOT NULL,
 start_time TIMESTAMP(6) NOT NULL,
 end_time TIMESTAMP(6),
 total_packages NUMBER(19) DEFAULT 0 NOT NULL,
 new_datasets NUMBER(19) DEFAULT 0 NOT NULL,
 new_resources NUMBER(19) DEFAULT 0 NOT NULL,
 updated_datasets NUMBER(19) DEFAULT 0 NOT NULL,
 updated_resources NUMBER(19) DEFAULT 0 NOT NULL,
 status VARCHAR2(20 CHAR),
 error_message CLOB,
 deleted_datasets NUMBER(19) DEFAULT 0 NOT NULL,
 deleted_resources NUMBER(19) DEFAULT 0 NOT NULL,
 FOREIGN KEY(instance_id) REFERENCES ckan_instance(id),
 outdated_resources NUMBER(19) DEFAULT 0 NOT NULL
);

CREATE TABLE resource_metadata_label (
 id VARCHAR2(36 CHAR) NOT NULL PRIMARY KEY,
 resource_id VARCHAR2(255 CHAR) NOT NULL,
 label VARCHAR2(100 CHAR) NOT NULL,
 created_at TIMESTAMP(6) NOT NULL
);

CREATE TABLE source_resource (
 id VARCHAR2(36 CHAR) NOT NULL,
 source_id VARCHAR2(36 CHAR) NOT NULL,
 parent_resource_id VARCHAR2(36 CHAR) NULL,
 kind VARCHAR2(16 CHAR) NOT NULL,
 title CLOB,
 description CLOB,
 page_url CLOB,
 content_url CLOB,
 format VARCHAR2(50 CHAR),
 source_version CLOB,
 dataset_name CLOB,
 datastore_active NUMBER(1) DEFAULT 0 NOT NULL,
 first_seen_sync VARCHAR2(36 CHAR) NOT NULL,
 last_changed_sync VARCHAR2(36 CHAR) NOT NULL,
 last_seen_sync VARCHAR2(36 CHAR) NOT NULL,
 deleted_at TIMESTAMP(6) NULL,
 created_at TIMESTAMP(6) NOT NULL,
 updated_at TIMESTAMP(6) NOT NULL,
 PRIMARY KEY(source_id,id),
 FOREIGN KEY(source_id) REFERENCES ckan_instance(id)
);

CREATE TABLE source_resource_version (
 id VARCHAR2(36 CHAR) PRIMARY KEY,
 resource_id VARCHAR2(36 CHAR) NOT NULL,
 source_id VARCHAR2(36 CHAR) NOT NULL,
 source_version CLOB,
 content_url CLOB,
 job_id VARCHAR2(36 CHAR) NOT NULL,
 status VARCHAR2(16 CHAR) DEFAULT 'discovered' NOT NULL,
 created_at TIMESTAMP(6) NOT NULL,
 updated_at TIMESTAMP(6) NOT NULL,
 FOREIGN KEY(source_id,resource_id) REFERENCES source_resource(source_id,id)
);

CREATE TABLE source_sync_outbox (
 sequence_id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
 id VARCHAR2(36 CHAR) NOT NULL UNIQUE,
 topic VARCHAR2(255 CHAR) NOT NULL,
 routing_key VARCHAR2(36 CHAR) NOT NULL,
 payload CLOB NOT NULL,
 delivered_at TIMESTAMP(6) NULL,
 created_at TIMESTAMP(6) NOT NULL
);

CREATE TABLE source_sync_run (
 sync_id VARCHAR2(36 CHAR) PRIMARY KEY,
 source_id VARCHAR2(36 CHAR) NOT NULL,
 status VARCHAR2(16 CHAR) NOT NULL,
 counts CLOB NOT NULL,
 completed_at TIMESTAMP(6) NOT NULL
);

CREATE TABLE repository_lock (lock_key VARCHAR2(128 CHAR) PRIMARY KEY);

CREATE INDEX ix_ckan_data_job_status ON ckan_data_job (status);

CREATE INDEX ix_ckan_data_job_resource_id ON ckan_data_job (resource_id);

CREATE INDEX ix_job_status_resource ON ckan_data_job (status,resource_id);

CREATE INDEX ix_ckan_data_job_instance_id ON ckan_data_job (instance_id);

CREATE INDEX ix_ckan_data_job_idempotency_key ON ckan_data_job (idempotency_key);

CREATE INDEX ix_ckan_data_job_parent ON ckan_data_job (parent_job_id,child_index);

CREATE INDEX ix_ckan_data_job_result_job_id ON ckan_data_job_result (job_id);

CREATE INDEX ix_latest_resource_job_latest_job_id ON latest_resource_job (latest_job_id);

CREATE INDEX ix_latest_resource_job_instance_id ON latest_resource_job (instance_id);

CREATE INDEX ix_last_terminal_status_last_terminal_job_id ON last_terminal_status (last_terminal_job_id);

CREATE INDEX ix_last_terminal_status_last_successful_job_id ON last_terminal_status (last_successful_job_id);

CREATE INDEX ix_metadata_sync_instance_id ON metadata_sync (instance_id);

CREATE UNIQUE INDEX uq_resource_label ON resource_metadata_label (resource_id,label);

CREATE INDEX ix_resource_metadata_label_resource_id ON resource_metadata_label (resource_id);

CREATE INDEX ix_source_resource_source ON source_resource (source_id,deleted_at);

CREATE INDEX ix_source_resource_parent ON source_resource (source_id,parent_resource_id);

CREATE INDEX ix_source_resource_version_job ON source_resource_version (job_id);

CREATE INDEX ix_source_sync_outbox_pending ON source_sync_outbox (delivered_at,sequence_id);
