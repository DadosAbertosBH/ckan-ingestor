-- +goose Up
INSERT IGNORE INTO ckan_instance (id,name,url,dataset_count,resource_count,created_at,updated_at) VALUES (UUID(),'PBH','https://dados.pbh.gov.br',0,0,UTC_TIMESTAMP(),UTC_TIMESTAMP()),(UUID(),'Minas Gerais','https://dados.mg.gov.br/',0,0,UTC_TIMESTAMP(),UTC_TIMESTAMP());

-- +goose Down
DELETE FROM ckan_instance WHERE name IN ('PBH','Minas Gerais') AND dataset_count = 0 AND resource_count = 0;
