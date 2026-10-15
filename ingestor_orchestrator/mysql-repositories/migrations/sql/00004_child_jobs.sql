-- Noctcloud Desenvolvimento LTDA
-- Copyright (C) 2026  Noctcloud Desenvolvimento LTDA
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- +goose Up
ALTER TABLE ckan_data_job
  ADD COLUMN parent_job_id varchar(36) NULL,
  ADD COLUMN parent_resource_id varchar(255) NULL,
  ADD COLUMN child_index int NULL,
  ADD COLUMN total_child_count int NULL,
  ADD COLUMN source_version varchar(255) NULL,
  ADD KEY ix_ckan_data_job_parent (parent_job_id, child_index),
  ADD CONSTRAINT fk_ckan_data_job_parent FOREIGN KEY (parent_job_id) REFERENCES ckan_data_job(id);
