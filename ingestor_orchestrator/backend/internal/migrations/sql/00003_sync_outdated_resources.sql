-- Noctcloud Desenvolvimento LTDA
-- Copyright (C) 2026  Noctcloud Desenvolvimento LTDA
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- +goose Up
ALTER TABLE metadata_sync ADD COLUMN outdated_resources int NOT NULL DEFAULT 0 AFTER updated_resources;
