# Pedalin CKAN ingestor

## Descrição 

Esse projeto é uma aplicação python que importa todos os conjuntos de dados (datasets) de uma aplicação [ckan](https://ckan.org) para lakehouse usando [delta lake](https://delta.io).


## Instruções de configuração 

1. Install `venv`  
   `pip install virtualenv`
2. Create the `venv`  
   `python3 -m venv env`
3. Active the `venv`  
   `source env/bin/activate`
4. Install python dependencies  
   `pip install -r requirements.txt`

## Rodando localmente

Para rodar localmente, importando os dados para uma instância [minio](https://min.io/) segui os seguintes passos:

1. Suba os containers necessários para rodar o projeto:  
   `docker compose up`
2. Dentro de um ambiente virtual ativo:
   `python ingestor/delta_standalone.py`

## Configurações

As tabela a seguir apresenta os possíveis parâmetros de configurações baseados em variáveis de ambientes:

| Variável de ambiente  | Valor padrão                |
|-----------------------| ----------------------------|
| AWS_ENDPOINT          | http://localhost:9000       |
| AWS_ACCESS_KEY_ID     | admin                       |
| AWS_SECRET_ACCESS_KEY | password                    |
| AWS_REGION            | us-west-1                   |
| S3_BUCKET             | public-datasets             |

---



bin/pulsar-admin sources create  --source-config-file  /pulsar/lake_house.json
bin/pulsar-admin sources get --tenant public --namespace default --name delta_source
bin/pulsar-client consume -s test-sub -n 0 delta_source