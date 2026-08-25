// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// ckan-ingestor-rs is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with ckan-ingestor-rs.  If not, see <https://www.gnu.org/licenses/>.

use anyhow::Result;

#[cfg(test)]
mod tests {
    #[test]
    fn installs_a_process_level_rustls_provider() {
        super::install_rustls_crypto_provider();
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    install_rustls_crypto_provider();

    std::panic::set_hook(Box::new(|info| {
        eprintln!("{}", info);
        std::process::abort();
    }));

    eprintln!("ckan-ingestor-consumer starting...");
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    ckan_ingestor_consumer::run().await
}

fn install_rustls_crypto_provider() {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        rustls::crypto::ring::default_provider()
            .install_default()
            .expect("failed to install the rustls crypto provider");
    }
}
