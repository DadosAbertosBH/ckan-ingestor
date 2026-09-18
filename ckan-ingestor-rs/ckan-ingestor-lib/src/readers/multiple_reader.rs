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
use crate::ckan_resource::CkanResource;
use crate::readers::ckan_reader::{CkanReader, FailedResult, HttpStatusError, ReadResult};
use log::info;
use reqwest::blocking::Client;
use reqwest::header::{CONTENT_DISPOSITION, CONTENT_TYPE};

/// Infers the format of a CKAN resource that has an empty `format` field.
///
/// Implementations inspect the remote resource (for example through an HTTP
/// header request) and return the CKAN format token readers understand.
pub trait FormatResolver {
    fn resolve(&self, resource: &CkanResource) -> Option<String>;
}

/// Resolves a missing format by issuing a `HEAD` request and inferring the
/// format from the response headers.
pub struct HttpFormatResolver {
    client: Client,
}

impl HttpFormatResolver {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

impl FormatResolver for HttpFormatResolver {
    fn resolve(&self, resource: &CkanResource) -> Option<String> {
        let response = self.client.head(&resource.url).send().ok()?;
        if !response.status().is_success() {
            return None;
        }
        let headers = response.headers();
        let content_type = headers
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok());
        let content_disposition = headers
            .get(CONTENT_DISPOSITION)
            .and_then(|value| value.to_str().ok());
        infer_format(content_type, content_disposition)
    }
}

/// Infers a CKAN format from the HTTP response headers of a resource.
///
/// The `Content-Type` header drives the match. When it is generic or missing
/// (for example `application/octet-stream`), the filename in
/// `Content-Disposition` provides the extension used instead.
pub fn infer_format(
    content_type: Option<&str>,
    content_disposition: Option<&str>,
) -> Option<String> {
    content_type
        .and_then(format_from_content_type)
        .or_else(|| content_disposition.and_then(format_from_content_disposition))
}

fn format_from_content_type(content_type: &str) -> Option<String> {
    let mime = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let format = match mime.as_str() {
        "text/csv" | "application/csv" | "application/x-csv" => "CSV",
        "text/tab-separated-values" => "TAB",
        "application/json" | "text/json" | "application/geo+json" | "application/ld+json" => "JSON",
        "text/html" | "application/xhtml+xml" => "HTML",
        "application/pdf" => "PDF",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document" => "DOCX",
        _ => return None,
    };
    Some(format.to_string())
}

fn format_from_content_disposition(content_disposition: &str) -> Option<String> {
    let filename = content_disposition.split(';').find_map(|part| {
        let (name, value) = part.trim().split_once('=')?;
        name.trim()
            .eq_ignore_ascii_case("filename")
            .then(|| value.trim().trim_matches('"'))
    })?;
    format_from_extension(filename)
}

fn format_from_extension(filename: &str) -> Option<String> {
    let extension = filename.rsplit_once('.')?.1.to_ascii_lowercase();
    let format = match extension.as_str() {
        "csv" => "CSV",
        "tsv" | "tab" => "TAB",
        "json" | "geojson" => "JSON",
        "html" | "htm" => "HTML",
        "pdf" => "PDF",
        "docx" => "DOCX",
        _ => return None,
    };
    Some(format.to_string())
}

/// Aggregate multiple types of a readers
/// into a single struct
pub struct MultipleReader<'a> {
    readers: Vec<Box<dyn CkanReader + 'a>>,
    supported_formarts: Vec<String>,
    format_resolver: Option<Box<dyn FormatResolver + 'a>>,
}

impl<'a> MultipleReader<'a> {
    pub fn new(readers: Vec<Box<dyn CkanReader + 'a>>) -> Self {
        let mut supported_formarts = Vec::new();
        for format in readers.iter().flat_map(|reader| reader.supported_formats()) {
            if !supported_formarts.contains(format) {
                supported_formarts.push(format.clone());
            }
        }
        Self {
            readers,
            supported_formarts,
            format_resolver: None,
        }
    }

    /// Sets the resolver used to infer the format when CKAN leaves it empty.
    pub fn with_format_resolver(mut self, resolver: impl FormatResolver + 'a) -> Self {
        self.format_resolver = Some(Box::new(resolver));
        self
    }

    fn resolved_resource(&self, resource: &CkanResource) -> Option<CkanResource> {
        if !resource.format.trim().is_empty() {
            return None;
        }
        let format = self.format_resolver.as_ref()?.resolve(resource)?;
        info!(
            "Inferred format {} for CKAN resource {} from its HTTP headers",
            format, resource.id
        );
        Some(CkanResource {
            format,
            ..resource.clone()
        })
    }
}

impl CkanReader for MultipleReader<'_> {
    fn supported_formats(&self) -> &[String] {
        &self.supported_formarts
    }

    fn do_read(&self, resource: &CkanResource) -> ReadResult {
        let resolved = self.resolved_resource(resource);
        let resource = resolved.as_ref().unwrap_or(resource);
        let mut last_failure = None;
        for reader in &self.readers {
            if !reader.can_read(resource) {
                continue;
            }
            log::info!(
                "Reading resource {} (format {}) with {}",
                resource.id,
                resource.format,
                reader.reader_name()
            );
            match reader.read(resource) {
                Ok(result) => {
                    return Ok(result);
                }
                Err(error) => {
                    info!(
                        "reader {} failed to read CKAN resource {}: {}",
                        reader.reader_name(),
                        resource.id,
                        error.error
                    );
                    if is_not_found(&error.error) {
                        return Err(error.into_deleted());
                    }
                    last_failure = Some(error);
                }
            };
        }
        if let Some(failure) = last_failure {
            return Err(failure);
        }
        let error = format!(
            "no reader could read CKAN resource {} (unsupported format)",
            resource.id
        );
        Err(FailedResult::from_string(
            &error,
            self.reader_name().to_string(),
        ))
    }
}

fn is_not_found(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<HttpStatusError>()
        .is_some_and(HttpStatusError::is_not_found)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::parquet_output::ParquetOutput;
    use arrow::{
        array::{ArrayRef, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    use super::*;

    struct TestReader {
        formats: Vec<String>,
        failure: Option<(reqwest::StatusCode, String)>,
        returns_data: bool,
    }

    fn output(batches: &[RecordBatch]) -> ParquetOutput {
        let mut output = ParquetOutput::try_new(&batches[0]).expect("valid Parquet output");
        for batch in batches {
            output.write(batch).expect("write Parquet batch");
        }
        output.finish().expect("finish Parquet output");
        output
    }

    impl TestReader {
        fn new(formats: &[&str], fails: bool) -> Self {
            Self {
                formats: formats.iter().map(|format| (*format).to_string()).collect(),
                failure: fails.then(|| {
                    (
                        reqwest::StatusCode::INTERNAL_SERVER_ERROR,
                        "test reader failed".to_string(),
                    )
                }),
                returns_data: false,
            }
        }

        fn with_data(formats: &[&str]) -> Self {
            Self {
                formats: formats.iter().map(|format| (*format).to_string()).collect(),
                failure: None,
                returns_data: true,
            }
        }

        fn failing_with_status(
            formats: &[&str],
            status: reqwest::StatusCode,
            message: &str,
        ) -> Self {
            Self {
                formats: formats.iter().map(|format| (*format).to_string()).collect(),
                failure: Some((status, message.to_string())),
                returns_data: false,
            }
        }
    }

    impl CkanReader for TestReader {
        fn supported_formats(&self) -> &[String] {
            &self.formats
        }

        fn do_read(&self, _resource: &CkanResource) -> ReadResult {
            if let Some((status, message)) = &self.failure {
                return Err(
                    anyhow::Error::new(HttpStatusError::new(*status, message.clone())).into(),
                );
            }
            let data = if self.returns_data {
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "value",
                    DataType::Utf8,
                    false,
                )]));
                vec![RecordBatch::try_new(
                    schema,
                    vec![Arc::new(StringArray::from(vec!["value"])) as ArrayRef],
                )
                .expect("valid test batch")]
            } else {
                let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
                vec![RecordBatch::try_new(
                    schema,
                    vec![Arc::new(StringArray::from(Vec::<String>::new())) as ArrayRef],
                )
                .expect("valid empty test batch")]
            };
            Ok(crate::readers::ckan_reader::SuccessResult::new(
                output(&data),
                self.reader_name().to_string(),
            ))
        }
    }

    struct CannotReadReader;

    impl CkanReader for CannotReadReader {
        fn supported_formats(&self) -> &[String] {
            static FORMATS: [String; 1] = [String::new()];
            &FORMATS
        }

        fn can_read(&self, _resource: &CkanResource) -> bool {
            false
        }

        fn do_read(&self, _resource: &CkanResource) -> ReadResult {
            panic!("do_read must not be called when can_read returns false");
        }
    }

    #[derive(Clone)]
    struct FakeResolver {
        format: Option<String>,
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl FakeResolver {
        fn returning(format: &str) -> Self {
            Self {
                format: Some(format.to_string()),
                calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            }
        }

        fn unable_to_resolve() -> Self {
            Self {
                format: None,
                calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl FormatResolver for FakeResolver {
        fn resolve(&self, _resource: &CkanResource) -> Option<String> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.format.clone()
        }
    }

    fn resource(format: &str) -> CkanResource {
        CkanResource {
            id: "resource-id".to_string(),
            package_id: String::new(),
            url: "https://example.test/resource".to_string(),
            format: format.to_string(),
            datastore_active: false,
            last_modified: String::new(),
        }
    }

    #[test]
    fn supported_formats_are_unique_in_first_seen_order() {
        let reader = MultipleReader::new(vec![
            Box::new(TestReader::new(&["CSV", "JSON"], false)),
            Box::new(TestReader::new(&["CSV", "PDF"], false)),
        ]);

        assert_eq!(reader.supported_formats(), &["CSV", "JSON", "PDF"]);
    }

    #[test]
    fn falls_back_to_the_next_reader_after_a_failure() {
        let reader = MultipleReader::new(vec![
            Box::new(TestReader::new(&["CSV"], true)),
            Box::new(TestReader::with_data(&["CSV"])),
        ]);

        assert!(reader.read(&resource("CSV")).is_ok());
    }

    #[test]
    fn falls_back_to_the_next_reader_after_empty_data() {
        let reader = MultipleReader::new(vec![
            Box::new(TestReader::new(&["CSV"], false)),
            Box::new(TestReader::with_data(&["CSV"])),
        ]);

        assert!(reader.read(&resource("CSV")).is_ok());
    }

    #[test]
    fn treats_an_empty_success_as_a_failure_for_reader_fallback() {
        let reader = TestReader::new(&["CSV"], false);

        let result = reader.read(&resource("CSV"));

        match result {
            Err(error) => assert_eq!(error.to_string(), "No data"),
            Ok(_) => panic!("an empty reader result must be treated as a failure"),
        }
    }

    #[test]
    fn skips_readers_that_cannot_read_the_resource() {
        let reader = MultipleReader::new(vec![
            Box::new(CannotReadReader),
            Box::new(TestReader::with_data(&["CSV"])),
        ]);

        assert!(reader.read(&resource("CSV")).is_ok());
    }

    #[test]
    fn rejects_an_unsupported_format_before_trying_readers() {
        let reader = MultipleReader::new(vec![Box::new(TestReader::new(&["CSV"], false))]);

        assert!(reader.read(&resource("PDF")).is_err());
    }

    #[test]
    fn returns_a_deleted_result_when_a_reader_fails_with_404() {
        let reader = MultipleReader::new(vec![
            Box::new(TestReader::failing_with_status(
                &["CSV"],
                reqwest::StatusCode::NOT_FOUND,
                "resource download failed with HTTP status 404 Not Found",
            )),
            Box::new(TestReader::with_data(&["CSV"])),
        ]);

        let result = reader.read(&resource("CSV"));

        match result {
            Err(error) => assert!(error.is_deleted()),
            Ok(_) => panic!("a 404 failure must be returned as a deleted result"),
        }
    }

    #[test]
    fn detects_an_http_404_error() {
        let not_found = anyhow::Error::new(HttpStatusError::new(
            reqwest::StatusCode::NOT_FOUND,
            "resource download failed with HTTP status 404 Not Found",
        ));
        let other = anyhow::Error::new(HttpStatusError::new(
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            "resource download failed with HTTP status 500 Internal Server Error",
        ));

        assert!(is_not_found(&not_found));
        assert!(!is_not_found(&other));
    }

    #[test]
    fn infers_the_format_from_the_content_type() {
        assert_eq!(infer_format(Some("text/csv"), None).as_deref(), Some("CSV"));
        assert_eq!(
            infer_format(Some("application/json; charset=utf-8"), None).as_deref(),
            Some("JSON")
        );
        assert_eq!(
            infer_format(Some("application/geo+json"), None).as_deref(),
            Some("JSON")
        );
        assert_eq!(
            infer_format(Some("text/tab-separated-values"), None).as_deref(),
            Some("TAB")
        );
        assert_eq!(
            infer_format(Some("text/html"), None).as_deref(),
            Some("HTML")
        );
        assert_eq!(
            infer_format(Some("application/pdf"), None).as_deref(),
            Some("PDF")
        );
        assert_eq!(
            infer_format(
                Some("application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
                None
            )
            .as_deref(),
            Some("DOCX")
        );
    }

    #[test]
    fn falls_back_to_the_content_disposition_filename() {
        assert_eq!(
            infer_format(
                Some("application/octet-stream"),
                Some("attachment; filename=\"data.csv\"")
            )
            .as_deref(),
            Some("CSV")
        );
        assert_eq!(
            infer_format(None, Some("attachment; filename=report.pdf")).as_deref(),
            Some("PDF")
        );
    }

    #[test]
    fn returns_no_format_when_the_headers_are_inconclusive() {
        assert!(infer_format(Some("application/octet-stream"), None).is_none());
        assert!(infer_format(None, None).is_none());
        assert!(infer_format(
            Some("application/octet-stream"),
            Some("attachment; filename=\"data.bin\"")
        )
        .is_none());
    }

    #[test]
    fn infers_the_format_when_the_resource_format_is_empty() {
        let reader = MultipleReader::new(vec![Box::new(TestReader::with_data(&["CSV"]))])
            .with_format_resolver(FakeResolver::returning("CSV"));

        assert!(reader.read(&resource("")).is_ok());
    }

    #[test]
    fn leaves_the_resource_format_untouched_when_it_is_present() {
        let resolver = FakeResolver::returning("PDF");
        let reader = MultipleReader::new(vec![Box::new(TestReader::with_data(&["CSV"]))])
            .with_format_resolver(resolver.clone());

        assert!(reader.read(&resource("CSV")).is_ok());
        assert_eq!(resolver.calls(), 0);
    }

    #[test]
    fn reports_an_unsupported_format_when_the_resolver_finds_none() {
        let reader = MultipleReader::new(vec![Box::new(TestReader::with_data(&["CSV"]))])
            .with_format_resolver(FakeResolver::unable_to_resolve());

        match reader.read(&resource("")) {
            Err(error) => assert!(
                error.to_string().contains("unsupported format"),
                "unexpected error: {error}"
            ),
            Ok(_) => panic!("an unresolved empty format must be reported as unsupported"),
        }
    }
}
