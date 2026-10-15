-- +goose Up
ALTER TABLE ckan_instance ADD COLUMN source_type VARCHAR(16) NOT NULL DEFAULT 'ckan';
INSERT IGNORE INTO ckan_instance (id,name,url,source_type,dataset_count,resource_count,created_at,updated_at)
VALUES ('4be9dd37-1f28-540d-a7c3-104e4f5a54e8','TCE-MG SICOM','https://dadosabertos.tce.mg.gov.br/','sicom',0,0,UTC_TIMESTAMP(),UTC_TIMESTAMP());

-- +goose Down
-- Keep the source row so existing jobs retain their instance foreign key.
ALTER TABLE ckan_instance DROP COLUMN source_type;
