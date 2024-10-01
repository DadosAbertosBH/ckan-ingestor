import duckdb

con = duckdb.connect(
    config={"custom_extension_repository": "http://nightly-extensions.duckdb.org"}
)
con.install_extension("uc_catalog")
con.load_extension("uc_catalog")