// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use std::process::Command;

#[test]
fn coordinator_dependency_tree_does_not_include_duckdb() {
    let output = Command::new(env!("CARGO"))
        .args(["tree", "-p", "ckan_worker_coordinator"])
        .current_dir(format!("{}/..", env!("CARGO_MANIFEST_DIR")))
        .output()
        .expect("cargo tree should run");
    assert!(output.status.success());
    let tree = String::from_utf8(output.stdout).expect("cargo tree should be UTF-8");

    assert!(
        !tree.lines().any(|line| line.contains("duckdb v")),
        "{tree}"
    );
    assert!(
        !tree.lines().any(|line| line.contains("libduckdb-sys v")),
        "{tree}"
    );
}

#[test]
fn worker_image_does_not_download_or_ship_duckdb() {
    let dockerfile = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../Dockerfile"))
        .expect("workspace Dockerfile should be readable");

    assert!(!dockerfile.to_ascii_lowercase().contains("duckdb"));
}

#[test]
fn worker_image_includes_every_workspace_crate() {
    let dockerfile = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../Dockerfile"))
        .expect("workspace Dockerfile should be readable");

    assert!(dockerfile.contains("message-processor/Cargo.toml"));
    assert!(dockerfile.contains("message-processor/src/"));
    assert!(dockerfile.contains("iggy-processor/Cargo.toml"));
    assert!(dockerfile.contains("iggy-processor/src/"));
}
