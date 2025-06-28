import duckdb
import pytest
from ckan_ingestor.duckdb_ckan_metadata_ingestor import DuckdbCkanMetadataIngestor

@pytest.fixture
def duckdb_conn():
    conn = duckdb.connect(database=':memory:')
    yield conn
    conn.close()

@pytest.fixture
def ingestor(duckdb_conn):
    return DuckdbCkanMetadataIngestor(duckdb_conn)

def test_get_outdated_resources_id(ingestor):
    conn = ingestor.conn
    # Cria as tabelas necessárias
    conn.execute('''
        CREATE TABLE ckan_resource (
            id varchar(50), last_modified varchar(50)
        )
    ''')
    conn.execute('''
        CREATE TABLE ckan_resource_last_update (
            ckan_resource_id varchar(50), last_modified TIMESTAMP
        )
    ''')
    # Insere dados de teste
    conn.execute("""
        INSERT INTO ckan_resource VALUES
        ('11111111-1111-1111-1111-111111111111', '2024-01-01 10:00:00'),
        ('22222222-2222-2222-2222-222222222222', '2024-01-02 10:00:00'),
        ('33333333-3333-3333-3333-333333333333', '2024-01-03 10:00:00')
    """)
    conn.execute("""
        INSERT INTO ckan_resource_last_update VALUES
        ('11111111-1111-1111-1111-111111111111', '2023-12-31 09:00:00'),
        ('22222222-2222-2222-2222-222222222222', '2025-01-02 09:00:00')
    """)
    # O recurso 111... está desatualizado (last_modified > last_update),
    # 222... tá atualizado (last_modified < last_update)
    # 333... nunca foi processado
    result = ingestor.get_outdated_resources_id()
    assert '11111111-1111-1111-1111-111111111111' in result
    assert '33333333-3333-3333-3333-333333333333' in result
    assert '22222222-2222-2222-2222-222222222222' not in result

