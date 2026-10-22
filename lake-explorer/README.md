# Observatório de dados

Portal em Vue/TypeScript com DuckDB-Wasm e catálogo DuckLake nativo. O navegador executa SQL e lê Parquet diretamente do R2 com credenciais temporárias. O PostgreSQL participa apenas da geração diária do catálogo.

## Desenvolvimento

Use Node 22.22+ ou Node 24.15+ e `uv` na raiz do repositório.

```sh
cd lake-explorer
corepack enable
pnpm install --frozen-lockfile
pnpm run dev
```

Para conectar ao R2, execute também `pnpm run preview` em outro terminal (Worker na porta 8787). O binding `BUCKET` usa o bucket R2 remoto, enquanto o Worker continua executando localmente. Configure `.dev.vars` com `R2_ACCESS_KEY_ID` e `R2_SECRET_ACCESS_KEY`, usando um token pai **somente de leitura** no bucket. Os valores permanecem nos secrets do Worker; o cliente recebe somente credenciais derivadas com validade de 15 minutos e permissão `GetObject`/`HeadObject` para o catálogo e os arquivos das tabelas consultadas. Tabelas com muitos arquivos usam escopo por diretório de dados, incluindo as versões armazenadas nesse diretório, para manter o token dentro do limite dos headers.

A busca usa nome, descrição e fonte dos metadados CKAN quando disponíveis. Tabela, resumo e gráfico usam filtros parametrizados. O editor SQL aceita consultas relacionais, tem destaque de sintaxe, autocomplete de tabelas/colunas e execução por Ctrl/Cmd+Enter, e guarda favoritos neste navegador. A exportação inclui o resultado exibido (100 linhas por página visual ou até 10.000 linhas no SQL).

O [catálogo genérico de fontes e recursos](../docs/source-catalog.md) fornece nomes, descrições e hierarquia para CKAN, SICOM e outros adaptadores. A navegação oferece busca global, seleção de fonte e coleção, filtro de disponibilidade e áreas de metadados e legado.

## Verificação reproduzível

Na raiz:

```sh
uv run python -c "import duckdb; duckdb.connect().execute('INSTALL ducklake')"
uv run pytest tests/ -q
uv run lake-explorer/e2e/create_fixture.py
```

No diretório `lake-explorer`:

```sh
pnpm test
pnpm run types
pnpm run check:worker
pnpm run check:deploy
pnpm exec playwright install chromium
pnpm run test:e2e
```

O teste Chromium usa um catálogo DuckLake real e uma origem S3 simulada: verifica session token, requisições `Range`, exclusões, filtros, SQL, dados inline, gráficos e downloads CSV/Parquet. A origem PostgreSQL já está desconectada quando o navegador abre o catálogo. O backend atual tem testes Go (`go test ./...`); o caminho legado `ingestor_orchestrator/backend/tests/` está ausente.

O Wasm fica no Web Worker. O build publica o engine comprimido (aproximadamente 8 MB), servido pelo Worker com `Content-Encoding: gzip`, dentro do limite de tamanho de assets. Flint/ECharts são carregados ao abrir o gráfico.

## Publicação diária

Imagem: `analytics/Dockerfile.snapshot`. O pipeline cria `lake-snapshot:<tag-ou-sha>` para amd64/arm64 e testa o frontend e o exportador.

O exportador recebe:

| Variável | Uso |
| --- | --- |
| `DUCKLAKE_CATALOG_URI` | URI PostgreSQL usada para copiar o catálogo em leitura |
| `DUCKLAKE_INGEST_CATALOG_URI` | URI com escrita para importar metadados; usa `DUCKLAKE_CATALOG_URI` quando omitida |
| `DUCKLAKE_METADATA_SCHEMA` | Schema do catálogo, normalmente `public` |
| `DUCKLAKE_DATA_PATH__ENDPOINT` | Endpoint R2 |
| `DUCKLAKE_DATA_PATH__BUCKET` | Bucket, padrão `public-datasets` |
| `DUCKLAKE_DATA_PATH__ACCESS_KEY_ID` | Leitura dos arquivos e publicação em `explorer/` |
| `DUCKLAKE_DATA_PATH__SECRET_ACCESS_KEY` | Secret correspondente |

O job primeiro importa as descobertas concluídas e seus resultados para as tabelas de catálogo no lake. Essa etapa usa credenciais com escrita no catálogo e nos Parquet de metadados. Em seguida copia as tabelas `ducklake_*` em uma transação de leitura, expira o histórico **na cópia** e regrava o catálogo para recuperar espaço. Preserva inline rows, deletes, schemas e arquivos originais. Valida contagens, publica o catálogo com checksum SHA-256, manifesto e índice de proteção; atualiza `explorer/latest.json` por último.

Habilite o componente pelo repositório GitOps, usando a versão do chart e da imagem que contêm esta implementação:

```yaml
lakeSnapshot:
  enabled: true
  existingSecret: lake-snapshot-credentials
  image:
    tag: "<tag-ou-sha-publicado>"
  schedule: "0 4 * * *"
  timeZone: America/Sao_Paulo
```

O secret específico deve conter a URI de leitura do catálogo e as credenciais do exportador. Ao usar o `existingSecret` global, o job obtém a URI do secret CNPG quando `postgres.deploy=true`; para um catálogo externo, inclua `DUCKLAKE_CATALOG_URI` no secret. As conexões de origem usam transações de leitura. Alterações no cluster seguem o fluxo Git → commit/push → ArgoCD.

### Retenção e manutenção

Cada arquivo publicado contém somente o último snapshot. O publicador mantém a publicação atual e anterior; outras publicações têm retenção de 48 horas mais 15 minutos para finalizar sessões. A limpeza automática remove exclusivamente os três artifacts daquela publicação em `explorer/<id>/`.

Cada publicação possui `protected-files.json`, com os arquivos de dados/deletes necessários e o índice por tabela. Toda rotina de limpeza de **dados do lake** deve obter a união de arquivos protegidos e excluir esses caminhos dos candidatos:

```sh
uv run python -m analytics.publish_lake_snapshot --protected-files
```

O código atual preserva os arquivos de dados. Para introduzir compaction/cleanup no lake de origem, coordene essa rotina com o exportador durante a cópia e a publicação e mantenha os arquivos referenciados por todas as publicações retidas. O índice representa um contrato de manutenção, não um lock distribuído.

`GET /api/health` retorna 503 quando a publicação passa de 36 horas; configure o monitor externo para alertar nesse endpoint. O exportador também registra ID, tamanho e duração. `GET /api/latest` permite avisar o visitante sobre novas publicações.

## Cloudflare e fechamento do acesso direto

1. Autentique Wrangler com `wrangler login` ou disponibilize `CLOUDFLARE_API_TOKEN` no ambiente de deploy.
2. Cadastre o token R2 pai de leitura com `npx wrangler secret put R2_ACCESS_KEY_ID` e `npx wrangler secret put R2_SECRET_ACCESS_KEY`.
3. Publique com `pnpm run deploy`; use o endereço HTTPS resultante como `portal_origin`.
4. Audite os domínios públicos existentes (`r2.dev` e custom domains) e os consumidores que os utilizam. A autenticação local estava expirada na implementação; o estado público atual segue pendente de confirmação.
5. Aplique `infra/main.tf.json` com `portal_origin` e o mapa `public_domains` (domínio → zone ID) encontrados na auditoria. A configuração desabilita `r2.dev` e cada domínio público listado e permite CORS de leitura apenas para a origem do portal.
6. Verifique objetos conhecidos sem assinatura (403), leitura com session token (200/206), rejeição de escrita e expiração após 15 minutos.

A infraestrutura pressupõe bucket existente. O plano Terraform deve ser revisado contra a configuração atual, preservando regras CORS necessárias aos consumidores autorizados. O provider consulta a Cloudflare; `terraform init -backend=false` e `terraform validate` verificam o schema localmente.

Visitantes anônimos continuam acessando os dados através do portal. Credenciais temporárias permitem leitura/download durante a sessão. CORS controla acesso entre origens no navegador; a autorização dos objetos vem da assinatura S3.

## Limites iniciais

Desktop é o foco; o layout também se adapta a telas menores. A memória da engine está limitada a 768 MB, os resultados SQL a 10.000 linhas e cada sessão pode solicitar até 50 tabelas por consulta. Catálogos são carregados integralmente; dados Parquet são lidos sob demanda. Grandes agregações ou tabelas com muitos arquivos continuam sujeitas aos limites do navegador e dos headers S3. O portal apresenta os erros do engine para orientar a redução da consulta.
