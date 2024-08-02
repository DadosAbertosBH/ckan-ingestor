# Pedalin CKAN ingestor

## Descrição 

Esse projeto é uma aplicação python que importa todos os conjuntos de dados (datasets) de uma aplicação [ckan](https://ckan.org) para lakehouse usando [delta lake](https://delta.io).

## Estrtutura do projeto

* <b>ingestor</b> Código fonte da aplicação
* <b>infra</b> terraform script para fazer o deploy do código
* <b>templates</b> templates de configuração da esteira do `gitlab`

## Instruções de configuração 

1. Entre na pasta com o código da aplicação
   `cd ingestor`
2. Install `venv`  
   `pip install virtualenv`
3. Create the `venv`  
   `python3 -m venv .venv`
4. Active the `venv`  
   `source .venv/bin/activate`
5. Install python dependencies  
   `pip install -r requirements.txt`

## Configurações

As tabela a seguir apresenta os possíveis parâmetros de configurações baseados em variáveis de ambientes:

| Variável de ambiente  | Valor padrão                |
|-----------------------| ----------------------------|
| AWS_ENDPOINT_URL      | http://localhost:9000       |
| AWS_ACCESS_KEY_ID     | admin                       |
| AWS_SECRET_ACCESS_KEY | password                    |
| AWS_REGION            | us-west-1                   |
| S3_BUCKET             | warehouse                   |

## Rodando localmente

Para rodar localmente, importando os dados para uma instância [minio](https://min.io/) segui os seguintes passos:

1. Suba os containers necessários para rodar o projeto:  
   `docker compose up`
2. Dentro de um ambiente virtual ativo:
   `python ingestor/delta_standalone.py`

## Rodando usando docker

`docker run --env AWS_ENDPOINT=https://mys3.endpoint  registry.gitlab.com/pedalin/ckan-ingestor:b69c2a35`

---

bin/pulsar-admin sources create  --source-config-file  /pulsar/lake_house.json  
bin/pulsar-admin sources get --tenant public --namespace default --name delta_source  
bin/pulsar-client consume -s test-sub -n 0 delta_source  