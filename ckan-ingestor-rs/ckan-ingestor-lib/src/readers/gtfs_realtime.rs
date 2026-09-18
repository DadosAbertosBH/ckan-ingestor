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

//! Decodes GTFS Realtime feeds from the binary protobuf wire format.
//!
//! The schema is the official `gtfs-realtime.proto`, compiled by `build.rs`
//! into the descriptor embedded below. Messages are decoded dynamically, so a
//! single reader supports the whole feed without generating Rust types.

use std::sync::OnceLock;

use anyhow::{anyhow, Context, Result};
use prost_reflect::{DescriptorPool, DynamicMessage, SerializeOptions};
use serde_json::Value;

/// Compiled GTFS Realtime schema produced by `build.rs`.
const DESCRIPTOR: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/gtfs-realtime.desc"));

/// Fully-qualified name of the GTFS Realtime root message.
const FEED_MESSAGE: &str = "transit_realtime.FeedMessage";

fn descriptor_pool() -> &'static DescriptorPool {
    static POOL: OnceLock<DescriptorPool> = OnceLock::new();
    POOL.get_or_init(|| {
        DescriptorPool::decode(DESCRIPTOR).expect("the embedded GTFS Realtime descriptor is valid")
    })
}

/// Decodes a GTFS Realtime feed into one JSON object per feed entity.
///
/// Each returned object mirrors a `FeedEntity`, so a feed with `n` entities
/// becomes `n` table rows with the nested trip, vehicle and stop updates kept
/// as nested columns.
pub(super) fn decode_feed_entities(bytes: &[u8]) -> Result<Vec<Value>> {
    let descriptor = descriptor_pool()
        .get_message_by_name(FEED_MESSAGE)
        .ok_or_else(|| anyhow!("the embedded descriptor has no {FEED_MESSAGE} message"))?;
    let message =
        DynamicMessage::decode(descriptor, bytes).context("invalid GTFS Realtime protobuf feed")?;
    if !message.has_field_by_name("header") {
        return Err(anyhow!(
            "protobuf payload is not a GTFS Realtime feed (missing FeedHeader)"
        ));
    }
    let mut serializer = serde_json::Serializer::new(Vec::new());
    message
        .serialize_with_options(&mut serializer, &serialize_options())
        .context("could not serialize the GTFS Realtime feed")?;
    let document: Value = serde_json::from_slice(&serializer.into_inner())?;
    Ok(document
        .get("entity")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default())
}

/// Serializes with the proto field names (`trip_id` instead of `tripId`) and
/// numeric 64-bit values, matching the GTFS Realtime schema and keeping
/// timestamps as numbers.
fn serialize_options() -> SerializeOptions {
    SerializeOptions::new()
        .use_proto_field_name(true)
        .stringify_64_bit_integers(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    const FEED: &[u8] = include_bytes!("../../tests/fixtures/data/gtfs.binpb");

    #[test]
    fn decodes_every_entity_of_a_valid_feed() -> Result<()> {
        let entities = decode_feed_entities(FEED)?;

        assert_eq!(entities.len(), 1290);
        assert_eq!(
            entities[0].get("id").and_then(Value::as_str),
            Some("21303 - 2661974S307679P141000")
        );
        Ok(())
    }

    #[test]
    fn keeps_nested_entity_data() -> Result<()> {
        let entities = decode_feed_entities(FEED)?;

        let trip_update = &entities[0]["trip_update"];
        assert_eq!(
            trip_update["trip"]["trip_id"].as_str(),
            Some("2661974S307679P141000")
        );
        assert_eq!(
            trip_update["stop_time_update"][0]["departure"]["delay"].as_i64(),
            Some(866)
        );
        Ok(())
    }

    #[test]
    fn rejects_invalid_protobuf_bytes() {
        assert!(decode_feed_entities(b"\x0a").is_err());
    }

    #[test]
    fn rejects_valid_protobuf_that_is_not_a_gtfs_feed() {
        // Field 1 as a varint is unknown to `FeedMessage` and leaves the
        // required `header` unset.
        let error = decode_feed_entities(b"\x08\x96\x01").unwrap_err();

        assert!(
            error.to_string().contains("GTFS Realtime"),
            "unexpected error: {error}"
        );
    }
}
