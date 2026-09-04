// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use ckan_ingestor_lib::duckdb_factory::{DuckdbConfig, DuckdbFactory};
use ckan_metadata_ingestor::{DuckdbCkanMetadataIngestor, MetadataSyncCommand};
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::Command;
use std::thread;

#[test]
#[ignore = "profiling test; run explicitly with -- --ignored --nocapture"]
fn reports_peak_rss_for_two_100_package_pages() {
    let profile = profile_sync(&["pbh_first_100.json", "pbh_offset_100.json"]);
    assert_eq!(profile.packages, 200);
}

#[test]
#[ignore = "profiling test; run explicitly with -- --ignored --nocapture"]
fn reports_peak_rss_for_one_200_package_page() {
    let profile = profile_sync(&["pbh_first_200.json"]);
    assert_eq!(profile.packages, 200);
}

#[test]
#[ignore = "profiling test; run explicitly with -- --ignored --nocapture"]
fn reports_peak_rss_for_four_consecutive_paginated_syncs() {
    let first_page = pbh_fixture_path("pbh_first_100.json");
    let second_page = pbh_fixture_path("pbh_offset_100.json");
    let responses = (0..4)
        .flat_map(|_| [Some(first_page.clone()), Some(second_page.clone()), None])
        .collect();
    let base_url = fixture_server_responses(responses);
    let temp_dir = tempfile::tempdir().unwrap();
    let factory = DuckdbFactory::new(DuckdbConfig::for_local_ducklake(
        temp_dir.path().join("catalog.ducklake").to_string_lossy(),
        temp_dir.path().join("data").to_string_lossy(),
    ));
    let conn = factory.open().unwrap();
    let command = MetadataSyncCommand {
        sync_id: "memory-profile".into(),
        instance_id: "pbh".into(),
        instance_name: "PBH".into(),
        instance_url: base_url,
    };
    let ingestor = DuckdbCkanMetadataIngestor::new(&conn);

    let results = (0..4)
        .map(|_| {
            let result = ingestor.sync(&command).unwrap();
            (result, peak_rss_bytes())
        })
        .collect::<Vec<_>>();

    eprintln!(
        "memory_profile={{sync_peaks: {:?}, packages: {}, resources: {}}}",
        results.iter().map(|(_, peak)| peak).collect::<Vec<_>>(),
        results[3].0.dataset_count,
        results[3].0.resource_count,
    );
    assert!(
        results
            .iter()
            .all(|(result, _)| result.dataset_count == 200)
    );
}

#[test]
#[ignore = "profiling test; run explicitly with -- --ignored --nocapture"]
fn paginated_fetch_uses_less_peak_rss_than_one_200_package_page() {
    let paginated = profile_in_subprocess("reports_peak_rss_for_two_100_package_pages");
    let one_page = profile_in_subprocess("reports_peak_rss_for_one_200_package_page");
    eprintln!(
        "peak_rss_bytes={{two_100_pages: {paginated}, one_200_page: {one_page}, reduction: {}}}",
        one_page.saturating_sub(paginated),
    );
    assert!(
        paginated < one_page,
        "two 100-package pages should use less memory than one 200-package page"
    );
}

struct MemoryProfile {
    packages: i64,
}

fn profile_sync(fixture_names: &[&str]) -> MemoryProfile {
    let fixture_paths = fixture_names
        .iter()
        .map(|name| pbh_fixture_path(name))
        .collect::<Vec<_>>();
    let base_url = fixture_server(fixture_paths);
    let temp_dir = tempfile::tempdir().unwrap();
    let factory = DuckdbFactory::new(DuckdbConfig::for_local_ducklake(
        temp_dir.path().join("catalog.ducklake").to_string_lossy(),
        temp_dir.path().join("data").to_string_lossy(),
    ));
    let conn = factory.open().unwrap();
    let initial_peak = peak_rss_bytes();
    let command = MetadataSyncCommand {
        sync_id: "memory-profile".into(),
        instance_id: "pbh".into(),
        instance_name: "PBH".into(),
        instance_url: base_url,
    };
    let result = DuckdbCkanMetadataIngestor::new(&conn)
        .sync(&command)
        .unwrap();
    let sync_peak = peak_rss_bytes();
    eprintln!(
        "memory_profile={{initial_peak: {initial_peak}, sync_peak: {sync_peak}, packages: {}, resources: {}}}",
        result.dataset_count, result.resource_count,
    );
    MemoryProfile {
        packages: result.dataset_count,
    }
}

fn profile_in_subprocess(test_name: &str) -> u64 {
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", test_name, "--nocapture"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "profile subprocess failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output = String::from_utf8_lossy(&output.stderr);
    let value = output
        .lines()
        .find_map(|line| line.split("sync_peak: ").nth(1))
        .and_then(|value| value.split(',').next())
        .and_then(|value| value.parse::<u64>().ok());
    value.expect("profile subprocess should report sync_peak")
}

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../ckan-ingestor-lib/tests/fixtures")
        .join(name)
}

fn pbh_fixture_path(name: &str) -> PathBuf {
    fixture_path("data").join(name)
}

fn fixture_server(fixtures: Vec<PathBuf>) -> String {
    let mut responses = fixtures.into_iter().map(Some).collect::<Vec<_>>();
    responses.push(None);
    fixture_server_responses(responses)
}

fn fixture_server_responses(responses: Vec<Option<PathBuf>>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    thread::spawn(move || {
        for response in responses {
            let (stream, _) = listener.accept().unwrap();
            match response {
                Some(fixture) => serve_fixture(stream, fixture).unwrap(),
                None => serve_empty_page(stream).unwrap(),
            }
        }
    });
    format!("http://{address}")
}

fn serve_fixture(stream: TcpStream, fixture: PathBuf) -> std::io::Result<()> {
    let fixture_length = std::fs::metadata(&fixture)?.len();
    let mut stream = read_request(stream)?;
    write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {fixture_length}\r\nConnection: close\r\n\r\n"
    )?;
    std::io::copy(&mut File::open(fixture)?, &mut stream)?;
    Ok(())
}

fn serve_empty_page(stream: TcpStream) -> std::io::Result<()> {
    let body = br#"{"result":[]}"#;
    let mut stream = read_request(stream)?;
    write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)?;
    Ok(())
}

fn read_request(stream: TcpStream) -> std::io::Result<TcpStream> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut request_line = String::new();
    reader.read_line(&mut request_line)?;
    assert!(request_line.starts_with("GET /api/action/current_package_list_with_resources?"));
    loop {
        let mut header = String::new();
        reader.read_line(&mut header)?;
        if header == "\r\n" {
            break;
        }
    }
    Ok(stream)
}

#[cfg(target_os = "macos")]
fn peak_rss_bytes() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    unsafe {
        libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr());
        usage.assume_init().ru_maxrss as u64
    }
}

#[cfg(target_os = "linux")]
fn peak_rss_bytes() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    unsafe {
        libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr());
        usage.assume_init().ru_maxrss as u64 * 1024
    }
}
