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

//! Compiles the GTFS Realtime schema into a descriptor pool embedded in the
//! library.
//!
//! The binary protobuf reader decodes dynamic messages from this pool, so the
//! `.proto` file stays the single source of truth for the supported schema
//! without requiring `protoc` at build time.

use std::path::PathBuf;

fn main() {
    let proto = PathBuf::from("src/gtfs-realtime.proto");
    println!("cargo:rerun-if-changed={}", proto.display());

    let file_descriptor_set = protox::Compiler::new(["src"])
        .expect("the proto include directory should be readable")
        .include_source_info(false)
        .include_imports(true)
        .open_files([proto])
        .expect("gtfs-realtime.proto should compile")
        .file_descriptor_set();

    let pool = protox::prost_reflect::DescriptorPool::from_file_descriptor_set(file_descriptor_set)
        .expect("the GTFS Realtime descriptor should be valid");

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").expect("cargo sets OUT_DIR"));
    std::fs::write(out_dir.join("gtfs-realtime.desc"), pool.encode_to_vec())
        .expect("the compiled GTFS Realtime descriptor should be written");
}
