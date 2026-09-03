// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        rustls::crypto::ring::default_provider()
            .install_default()
            .expect("failed to install the rustls crypto provider");
    }
    std::panic::set_hook(Box::new(|info| {
        eprintln!("{info}");
        std::process::abort();
    }));
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    ckan_worker_coordinator::run().await
}
