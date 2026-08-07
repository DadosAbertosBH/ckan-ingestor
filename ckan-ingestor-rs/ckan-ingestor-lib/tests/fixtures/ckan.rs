pub mod ckan_mock {
    use rstest::fixture;
    use std::path::Path;
    use wiremock::matchers::{method, path_regex, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    pub struct CkanMock {
        pub server: MockServer,
    }

    #[fixture]
    pub async fn ckan_mock() -> CkanMock {
        let server = MockServer::start().await;

        // Mock PDF
        Mock::given(method("GET"))
            .and(path_regex(r"/datastore/[^/]+"))
            .and(query_param("format", "PDF")) // aceita qualquer valor para format
            .respond_with(|req: &wiremock::Request| {
                let path = req.url.path().strip_prefix("/datastore/").unwrap();
                let format = req
                    .url
                    .query_pairs()
                    .find(|(key, _)| key == "format")
                    .map(|(_, value)| value.to_string())
                    .unwrap();

                let file_path = Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("tests")
                    .join("fixtures")
                    .join("data")
                    .join(format!("{}.{}", path, format.to_lowercase()));

                let content = std::fs::read(file_path).unwrap();
                ResponseTemplate::new(200)
                    .set_body_bytes(content)
                    .insert_header("Content-Type", "*/*")
            })
            .mount(&server)
            .await;

        // Mock para offset=0
        Mock::given(method("GET"))
            .and(path_regex(r"/datastore/[^/]+"))
            .and(query_param("format", "*")) // aceita qualquer valor para format
            .and(query_param("offset", "0"))
            .respond_with(|req: &wiremock::Request| {
                let path = req.url.path().strip_prefix("/datastore/").unwrap();
                let format = req
                    .url
                    .query_pairs()
                    .find(|(key, _)| key == "format")
                    .map(|(_, value)| value.to_string())
                    .unwrap();

                let file_path = Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("tests")
                    .join("fixtures")
                    .join("data")
                    .join(format!("{}.{}", path, format.to_lowercase()));

                let content = std::fs::read_to_string(file_path).unwrap();
                ResponseTemplate::new(200)
                    .set_body_string(content)
                    .insert_header("Content-Type", "*/*")
            })
            .mount(&server)
            .await;

        // Mock para offset diferente de 0
        Mock::given(method("GET"))
            .and(path_regex(r"/datastore/[^/]+"))
            .and(query_param("format", "*")) // aceita qualquer valor para format
            .and(query_param("offset", "*")) // aceita qualquer valor para offset exceto 0
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(
                        std::fs::read_to_string(
                            Path::new(env!("CARGO_MANIFEST_DIR"))
                                .join("tests")
                                .join("fixtures")
                                .join("data")
                                .join("empty.json"),
                        )
                        .unwrap(),
                    )
                    .insert_header("Content-Type", "*/*"),
            )
            .mount(&server)
            .await;

        CkanMock { server }
    }
}
