from ckan_ingestor.config.ducklake_settings import DucklakeSettings
from ckan_ingestor.duckdb_connection_factory import from_settings

def test_from_settings_idempotent():
    """Testa se executar from_settings duas vezes não gera erro de attach duplicado."""
    settings = DucklakeSettings()  # Use valores padrão ou mock conforme necessário
    conn1 = from_settings(settings)
    conn2 = from_settings(settings)

    conn1.close()
    conn2.close()
