# ckan-ingestor-rs

Rust utilities for ingesting CKAN datasets using DuckDB.

Features:
- HTTP-based dataset fetcher
- CSV and datastore readers backed by DuckDB
- S3 PDF document ingestor

## Running tests

```bash
cargo fmt -- --check
cargo clippy -- -D warnings
cargo test
```

## License

Distributed under the GNU Affero General Public License v3.0. See [LICENSE](LICENSE) for full text.
