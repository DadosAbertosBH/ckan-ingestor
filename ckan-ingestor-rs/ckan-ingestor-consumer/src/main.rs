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

mod coordinator_consumer_context;
mod message_source;
mod messages;
mod result_publisher;
mod worker;
mod worker_coordinator;
mod worker_thread;

use anyhow::Result;
use log::info;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    // Panics in spawned tasks don't kill the process by default.
    // We need abort-on-panic so that Docker/K8s restarts the worker,
    // re-delivering the failed message for retry.
    std::panic::set_hook(Box::new(|info| {
        eprintln!("{}", info);
        std::process::abort();
    }));

    eprintln!("ckan-ingestor-consumer starting...");
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    info!("CKAN Ingestor Consumer starting...");

    let worker = Arc::new(worker::Worker::new()?);
    worker.run().await?;

    Ok(())
}
