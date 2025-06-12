import duckdb

from ckan_ingestor.config.ducklake_settings import DucklakeSettings


def from_settings(settings: DucklakeSettings = DucklakeSettings()):
    """Return a duckdb connection with required extensions."""
    conn = duckdb.connect(settings.database)
    conn.install_extension("ducklake")
    conn.load_extension("ducklake")
    conn.execute("INSTALL postgres; LOAD postgres;")
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute("SET pg_debug_show_queries=false;")

    account_id = "" if settings.data_path.account_id is None \
        else f",'ACCOUNT_ID '{settings.data_path.account_id}'"

    stmt = f"""
            CREATE OR REPLACE SECRET secret (
                TYPE '{settings.data_path.protocol}',
                ENDPOINT '{settings.data_path.endpoint}',
                KEY_ID '{settings.data_path.access_key_id}',
                SECRET '{settings.data_path.secret_access_key}',
                USE_SSL '{settings.data_path.use_ssl}',
                URL_STYLE '{settings.data_path.url_style}'
                {account_id}
            );
        """

    conn.execute(stmt)

    stmt = (
        "ATTACH 'ducklake:{conn}' AS lake (DATA_PATH '{data_path_protocol}://{data_path_bucket}');"

    ).format(
        conn=settings.catalog_uri,
        data_path_protocol=settings.data_path.protocol,
        data_path_bucket=settings.data_path.bucket,
    )

    conn.execute(stmt)
    conn.execute("USE lake;")
    return conn