# Catálogo de fontes e recursos

Uma instância é um **Source**. Seus recursos formam uma árvore por `parent_resource_id`: packages CKAN são coleções; categorias e anos SICOM também são coleções. A identidade é o par `(source_id, id)`, preservando o identificador original da API.

O adaptador fornece os campos comuns, o objeto original em `metadata` e, opcionalmente, `metadata_collection`, o nome da tabela compartilhada que receberá esse JSON. O núcleo usa o mesmo mecanismo para todas as coleções declaradas. A descoberta fica em um arquivo temporário até a validação de identidades, pais e ciclos. Após a consolidação operacional, o source-sync-worker publica um artefato imutável no R2:

```text
catalog-inbox/<source_id>/<sync_id>.jsonl
```

A primeira linha identifica a coleta; as seguintes representam recursos:

```json
{"version":1,"sync_id":"coleta-1","collected_at":"2026-10-22T12:00:00Z","resources_total":2,"source":{"id":"portal-1","title":"Portal municipal","type":"ckan","url":"https://dados.example"}}
{"source_id":"portal-1","id":"package-1","kind":"collection","title":"Despesas","metadata_collection":"ckan_packages","metadata":{"license_id":"cc-by"}}
{"source_id":"portal-1","id":"resource-1","parent_resource_id":"package-1","kind":"file","title":"Despesas 2026","content_url":"https://dados.example/2026.csv","format":"CSV","metadata_collection":"ckan_resources","metadata":{"campo_especifico":"valor"}}
```

O PUT condicional preserva a primeira publicação de uma coleta. Uma descoberta completa com zero recursos é válida e registra as remoções daquela fonte.

## Ingestão e consulta

Antes da publicação diária, `analytics.catalog_ingestion` importa os artefatos pendentes. `replace_collection` é o mecanismo genérico para substituir os registros de uma fonte em uma coleção de JSON; a transação da coleta inclui o estado atual e sua identificação. Reprocessamentos e coletas antigas preservam o estado consolidado. Os artefatos imutáveis em `catalog-inbox/` mantêm a trilha de auditoria completa.

As tabelas consultáveis são:

| Tabela | Conteúdo |
| --- | --- |
| `catalog_sources` | Fontes e nomes de apresentação |
| `catalog_resources` | Árvore, campos comuns, JSON original, estado e tombstones |
| `catalog_tables` | Relação entre fonte/recurso e tabelas físicas |
| `catalog_imports` | Coletas consolidadas |
| `catalog_collections` | Coleções de metadados declaradas pelos adaptadores |
| `ckan_packages`, `ckan_resources` | Listagens compartilhadas entre instâncias, com JSON original, coleta e data |

```sql
SELECT source_id, id, metadata->>'license_id' AS license
FROM lake.main.ckan_packages;
```

Cada resource possui UUID global e esse próprio `resource_id` é o nome da tabela física e o prefixo dos seus Parquets. As tags `source_id`/`resource_id` registram o vínculo de catálogo e permitem associar várias tabelas a um resource. Tabelas existentes são preservadas e vinculadas diretamente pelo identificador. As demais ficam na área **Legado**, disponíveis também pelo SQL.

O status de disponibilidade é derivado dessas tabelas e tags: uma tabela cuja `source_version` coincide com o resource atual é apresentada como concluída. Os detalhes de tentativas e falhas de jobs continuam no banco operacional e no monitoramento.

## Publicação e implantação

O manifesto inclui fontes, recursos, caminhos e vínculos obtidos do mesmo catálogo copiado. O Lake Explorer usa esses campos para busca global, navegação por fonte/coleção e filtros de disponibilidade; as tabelas de catálogo aparecem em **Metadados**. Publicações anteriores continuam abrindo.

Publicar as imagens de source-sync-worker, coordenador e lake-snapshot e o frontend, seguindo o fluxo GitOps do projeto. O worker usa `S3_ENDPOINT`, `S3_BUCKET`, `S3_ACCESS_KEY_ID` e `S3_SECRET_ACCESS_KEY` existentes. Seu acesso deve permitir PUT condicional em `catalog-inbox/`.

O job lake-snapshot precisa listar/ler esses prefixos e escrever as tabelas de catálogo no DuckLake. Configure `DUCKLAKE_INGEST_CATALOG_URI` com um usuário de escrita se `DUCKLAKE_CATALOG_URI` usa um usuário de leitura. A cópia para publicação continua usando `DUCKLAKE_CATALOG_URI` em leitura. As credenciais R2 do job também devem permitir gravar os Parquet de metadados, além das publicações em `explorer/`.

As próximas sincronizações completam o JSON original e a hierarquia das fontes existentes. Os artefatos são preservados; sua retenção exige uma política posterior. Falhas de importação interrompem a publicação e preservam `explorer/latest.json`; o monitor de 36 horas existente continua detectando publicações atrasadas.
