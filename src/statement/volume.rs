// Copyright (c) 2026 Databricks, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::client::retry::{calculate_backoff, RequestType, RetryStrategy};
use crate::client::transport::ExecuteResult;
use crate::client::DatabricksHttpClient;
use crate::kernel_error::{Error, ErrorCode, Result};
use crate::reader::{EmptyReader, InMemoryReader};
use crate::result_stream::{ResultBatch, ResultStream};
use crate::statement::StatementSpec;
use arrow_array::{Array, MapArray, RecordBatch, StringArray};
use futures::StreamExt;
use reqwest::Method;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;
use tokio::sync::OnceCell;
use tokio_util::io::ReaderStream;

#[derive(Clone, Default)]
pub(crate) struct VolumeOptions {
    paths: Option<Vec<String>>,
    retain_result: bool,
}

impl From<&StatementSpec> for VolumeOptions {
    fn from(spec: &StatementSpec) -> Self {
        Self {
            paths: spec.staging_allowed_local_paths.clone(),
            retain_result: spec.retain_volume_operation_result,
        }
    }
}

pub(crate) async fn complete(
    client: &DatabricksHttpClient,
    mut result: ExecuteResult,
    options: &VolumeOptions,
    completion: &OnceCell<Result<()>>,
) -> Result<ExecuteResult> {
    let Some(paths) = &options.paths else {
        return Ok(result);
    };
    if !result
        .manifest
        .as_ref()
        .is_some_and(|m| m.is_volume_operation)
    {
        return Ok(result);
    }
    let statement_id = result.statement_id.clone();
    async {
        let mut stream = ResultStream::from_reader(result.reader).await?;
        let schema = stream.schema();
        let batch = match stream.next_batch().await? {
            Some(ResultBatch::Arrow(batch)) if batch.num_rows() == 1 => batch.clone(),
            _ => {
                return Err(Error::invalid_state().message("Expected one file transfer instruction"))
            }
        };
        let row = stream
            .next_row_in_batch()
            .ok_or_else(|| Error::invalid_state().message("Missing file transfer instruction"))?;
        if !(3..=4).contains(&row.num_columns()) {
            return Err(Error::invalid_state().message("Invalid file transfer instruction columns"));
        }
        let operation = row.get_str(0)?.unwrap_or_default().to_owned();
        let url = row.get_str(1)?.unwrap_or_default().to_owned();
        let headers = instruction_headers(&batch)?;
        let local_file = if row.num_columns() == 4 {
            row.get_str(3)?.map(str::to_owned)
        } else {
            None
        };
        if stream.next_batch().await?.is_some() {
            return Err(Error::invalid_state().message("Expected one file transfer instruction"));
        }
        completion
            .get_or_init(|| {
                transfer(
                    client,
                    &operation,
                    &url,
                    &headers,
                    local_file.as_deref(),
                    paths,
                )
            })
            .await
            .clone()?;
        result.reader = if options.retain_result {
            Box::new(InMemoryReader::new(schema, vec![batch]))
        } else {
            Box::new(EmptyReader::new(schema))
        };
        Ok(result)
    }
    .await
    .map_err(|e: Error| {
        e.with_error_code("FILE_TRANSFER_ERROR")
            .with_sql_state("HY000")
            .with_query_id(&statement_id)
    })
}

fn instruction_headers(batch: &RecordBatch) -> Result<HashMap<String, String>> {
    let invalid = || Error::invalid_state().message("Invalid file transfer headers");
    let column = batch.column(2);
    if column.is_null(0) {
        return Ok(HashMap::new());
    }
    if let Some(map) = column.as_any().downcast_ref::<MapArray>() {
        let entries = map.value(0);
        let keys = entries
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(invalid)?;
        let values = entries
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(invalid)?;
        keys.iter()
            .zip(values.iter())
            .map(|(key, value)| {
                Ok((
                    key.ok_or_else(invalid)?.to_owned(),
                    value.ok_or_else(invalid)?.to_owned(),
                ))
            })
            .collect()
    } else if let Some(text) = column.as_any().downcast_ref::<StringArray>() {
        serde_json::from_str(text.value(0)).map_err(|_| invalid())
    } else {
        Err(invalid())
    }
}

fn file_error(error: std::io::Error) -> Error {
    Error::new(
        if error.kind() == std::io::ErrorKind::NotFound {
            ErrorCode::NotFound
        } else {
            ErrorCode::InvalidArgument
        },
        format!("File transfer failed: {error}"),
    )
}

fn resolved_path(path: &Path) -> Result<PathBuf> {
    match path.canonicalize() {
        Ok(path) => Ok(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let absolute = std::path::absolute(path).map_err(file_error)?;
            let parent = absolute
                .parent()
                .ok_or_else(|| Error::invalid_argument().message("Invalid local file path"))?;
            let name = absolute
                .file_name()
                .ok_or_else(|| Error::invalid_argument().message("Invalid local file path"))?;
            Ok(parent.canonicalize().map_err(file_error)?.join(name))
        }
        Err(error) => Err(file_error(error)),
    }
}

fn allowed_path(local_file: Option<&str>, paths: &[String]) -> Result<PathBuf> {
    let file = local_file.filter(|s| !s.is_empty()).ok_or_else(|| {
        Error::invalid_argument().message("Local file path is required for PUT and GET")
    })?;
    let file = resolved_path(Path::new(file))?;
    for base in paths {
        let Ok(base) = resolved_path(Path::new(base)) else {
            continue;
        };
        if file.starts_with(base) {
            return Ok(file);
        }
    }
    Err(Error::new(
        ErrorCode::PermissionDenied,
        "Local file operations are restricted to the configured allowed paths",
    ))
}

async fn transfer(
    client: &DatabricksHttpClient,
    operation: &str,
    url: &str,
    headers: &HashMap<String, String>,
    local_file: Option<&str>,
    paths: &[String],
) -> Result<()> {
    let method = match operation {
        "PUT" => Method::PUT,
        "GET" => Method::GET,
        "REMOVE" => Method::DELETE,
        _ => return Err(Error::invalid_state().message("Unsupported file transfer operation")),
    };
    let parsed = reqwest::Url::parse(url)
        .map_err(|_| Error::invalid_state().message("Invalid file transfer URL"))?;
    let loopback = parsed.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    if parsed.scheme() != "https" && !(parsed.scheme() == "http" && loopback) {
        return Err(Error::invalid_state().message("Invalid file transfer URL scheme"));
    }
    let local = if operation == "REMOVE" {
        None
    } else {
        Some(allowed_path(local_file, paths)?)
    };
    if operation == "GET"
        && local
            .as_ref()
            .is_some_and(|path| path.parent().is_none() || path.is_dir())
    {
        return Err(Error::invalid_argument().message("Download destination must be a file"));
    }
    let response = send_transfer(client, method, parsed, headers, local.as_deref()).await?;
    if !response.status().is_success()
        && !(operation == "REMOVE" && response.status().as_u16() == 404)
    {
        return Err(
            Error::from_http_status(response.status().as_u16()).message(format!(
                "File transfer failed with HTTP {}",
                response.status().as_u16()
            )),
        );
    }
    if operation == "GET" {
        let destination = local.as_ref().unwrap();
        let parent = destination.parent().ok_or_else(|| {
            Error::invalid_argument().message("Download destination must be a file")
        })?;
        let builder = tempfile::Builder::new();
        #[cfg(unix)]
        let builder = {
            use std::os::unix::fs::PermissionsExt;
            let mut builder = builder;
            builder.permissions(std::fs::Permissions::from_mode(0o666));
            builder
        };
        let temporary = builder.tempfile_in(parent).map_err(file_error)?;
        let mut file = tokio::fs::File::from_std(temporary.reopen().map_err(file_error)?);
        let mut body = response.bytes_stream();
        while let Some(bytes) = body.next().await {
            let bytes = bytes.map_err(transport_error)?;
            file.write_all(&bytes).await.map_err(file_error)?;
        }
        file.flush().await.map_err(file_error)?;
        drop(file);
        match std::fs::metadata(destination) {
            Ok(metadata) => temporary
                .as_file()
                .set_permissions(metadata.permissions())
                .map_err(file_error)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(file_error(error)),
        }
        temporary
            .persist(destination)
            .map_err(|error| file_error(error.error))?;
    }
    Ok(())
}

fn transport_error(error: reqwest::Error) -> Error {
    let message = if error.is_timeout() {
        "File transfer timed out"
    } else if error.is_connect() {
        "File transfer connection failed"
    } else if error.is_body() || error.is_decode() {
        "File download interrupted"
    } else {
        "File transfer request failed"
    };
    Error::new(ErrorCode::NetworkError, message).with_cause(Error::new(
        ErrorCode::NetworkError,
        format!("{:?}", error.without_url()),
    ))
}

async fn send_transfer(
    client: &DatabricksHttpClient,
    method: Method,
    url: reqwest::Url,
    headers: &HashMap<String, String>,
    local: Option<&Path>,
) -> Result<reqwest::Response> {
    let config = client.storage_retry_config();
    let strategy = RetryStrategy::for_request(RequestType::CloudFetchDownload, &config);
    let started = std::time::Instant::now();
    for attempt in 0..=config.max_retries {
        let remaining = config.overall_timeout.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return Err(Error::new(
                ErrorCode::Timeout,
                "File transfer deadline exceeded",
            ));
        }
        // Large files need the storage budget rather than the SQL request timeout.
        let mut request = client
            .inner()
            .request(method.clone(), url.clone())
            .timeout(remaining);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        if method == Method::PUT {
            let local = local
                .ok_or_else(|| Error::invalid_argument().message("Missing local upload path"))?;
            let metadata = tokio::fs::metadata(local).await.map_err(file_error)?;
            if !metadata.is_file() {
                return Err(Error::invalid_argument().message("Local upload path must be a file"));
            }
            // Each retry reopens the file so a streaming upload starts at byte zero.
            let file = tokio::fs::File::open(local).await.map_err(file_error)?;
            request = request
                .header(reqwest::header::CONTENT_LENGTH, metadata.len())
                .body(reqwest::Body::wrap_stream(ReaderStream::new(file)));
        }
        // Presigned storage requests must omit workspace credentials and custom headers.
        let response = request.send().await;
        let (retryable, retry_after) = match &response {
            Ok(response) => (
                strategy.is_retryable_status(
                    response.status(),
                    response
                        .headers()
                        .contains_key(reqwest::header::RETRY_AFTER),
                ),
                response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|h| h.to_str().ok()),
            ),
            Err(error) => (strategy.is_retryable_error(error), None),
        };
        if !retryable || attempt == config.max_retries {
            return response.map_err(transport_error);
        }
        let delay = calculate_backoff(&config, attempt, retry_after);
        if started.elapsed().saturating_add(delay) >= config.overall_timeout {
            return response.map_err(transport_error);
        }
        tokio::time::sleep(delay).await;
    }
    unreachable!("retry loop always attempts the request")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::HttpClientConfig;
    use arrow_array::builder::{MapBuilder, StringBuilder};
    use std::sync::Arc;
    use wiremock::matchers::{body_bytes, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn client() -> DatabricksHttpClient {
        DatabricksHttpClient::with_default_retry(HttpClientConfig::default()).unwrap()
    }

    fn instruction(operation: &str, url: &str, local: &str) -> RecordBatch {
        let mut headers = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        headers.keys().append_value("x-file-header");
        headers.values().append_value("value");
        headers.append(true).unwrap();
        RecordBatch::try_from_iter(vec![
            (
                "operation",
                Arc::new(StringArray::from(vec![operation])) as arrow_array::ArrayRef,
            ),
            ("url", Arc::new(StringArray::from(vec![url]))),
            ("headers", Arc::new(headers.finish())),
            ("local", Arc::new(StringArray::from(vec![local]))),
        ])
        .unwrap()
    }

    fn result(batch: RecordBatch, marker: bool) -> ExecuteResult {
        ExecuteResult {
            statement_id: "statement".into(),
            reader: Box::new(InMemoryReader::new(batch.schema(), vec![batch])),
            manifest: Some(
                serde_json::from_value(serde_json::json!({
                    "format": "ARROW_STREAM", "schema": {"column_count": 4},
                    "is_volume_operation": marker
                }))
                .unwrap(),
            ),
            telemetry_result_format: crate::telemetry::ExecutionResultFormat::InlineArrow,
            telemetry_is_compressed: false,
            status: serde_json::from_value(serde_json::json!({"state":"SUCCEEDED"})).unwrap(),
        }
    }

    #[tokio::test]
    async fn completes_transfer_once_and_preserves_requested_result() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/file"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"file contents"))
            .expect(1)
            .mount(&server)
            .await;
        let dir = tempfile::tempdir().unwrap();
        let local = dir.path().join("download");
        let batch = instruction(
            "GET",
            &format!("{}/file", server.uri()),
            local.to_str().unwrap(),
        );
        let options = VolumeOptions {
            paths: Some(vec![dir.path().to_str().unwrap().into()]),
            retain_result: true,
        };
        let completion = OnceCell::new();
        let http = client();
        for _ in 0..2 {
            let completed = complete(&http, result(batch.clone(), true), &options, &completion)
                .await
                .unwrap();
            let mut reader = ResultStream::from_reader(completed.reader).await.unwrap();
            assert!(reader.next_batch().await.unwrap().is_some());
            assert!(reader.next_batch().await.unwrap().is_none());
        }
        assert_eq!(tokio::fs::read(local).await.unwrap(), b"file contents");
        let request = server.received_requests().await.unwrap().remove(0);
        assert!(!request.headers.contains_key("authorization"));
        assert_eq!(request.headers["x-file-header"], "value");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn download_preserves_file_permissions_and_rejects_root() {
        use std::os::unix::fs::PermissionsExt;
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"contents"))
            .expect(2)
            .mount(&server)
            .await;
        let dir = tempfile::tempdir().unwrap();
        let destination = dir.path().join("download");
        let control = dir.path().join("control");
        std::fs::write(&control, b"").unwrap();
        let roots = vec![dir.path().to_str().unwrap().into()];
        for existing in [false, true] {
            if existing {
                std::fs::set_permissions(&destination, std::fs::Permissions::from_mode(0o640))
                    .unwrap();
            }
            transfer(
                &client(),
                "GET",
                &server.uri(),
                &HashMap::new(),
                destination.to_str(),
                &roots,
            )
            .await
            .unwrap();
            let expected = if existing {
                0o640
            } else {
                std::fs::metadata(&control).unwrap().permissions().mode() & 0o777
            };
            assert_eq!(
                std::fs::metadata(&destination)
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                expected
            );
        }
        for destination in [Path::new("/"), dir.path()] {
            let error = transfer(
                &client(),
                "GET",
                &server.uri(),
                &HashMap::new(),
                destination.to_str(),
                &["/".into()],
            )
            .await
            .unwrap_err();
            assert_eq!(error.code, ErrorCode::InvalidArgument);
        }
    }

    #[tokio::test]
    async fn streams_upload_and_accepts_missing_remove() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/file"))
            .and(body_bytes(b"upload"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/file"))
            .respond_with(ResponseTemplate::new(404))
            .expect(1)
            .mount(&server)
            .await;
        let dir = tempfile::tempdir().unwrap();
        let local = dir.path().join("input");
        tokio::fs::write(&local, b"upload").await.unwrap();
        let roots = vec![dir.path().to_str().unwrap().into()];
        let http = client();
        transfer(
            &http,
            "PUT",
            &format!("{}/file", server.uri()),
            &HashMap::new(),
            local.to_str(),
            &roots,
        )
        .await
        .unwrap();
        transfer(
            &http,
            "REMOVE",
            &format!("{}/file", server.uri()),
            &HashMap::new(),
            None,
            &[],
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn retries_upload_from_the_start() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let server = MockServer::start().await;
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&attempts);
        Mock::given(method("PUT"))
            .and(path("/file"))
            .and(body_bytes(b"upload"))
            .respond_with(move |_: &wiremock::Request| {
                ResponseTemplate::new(if observed.fetch_add(1, Ordering::SeqCst) == 0 {
                    503
                } else {
                    200
                })
            })
            .expect(2)
            .mount(&server)
            .await;
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("input");
        std::fs::write(&file, b"upload").unwrap();
        transfer(
            &client(),
            "PUT",
            &format!("{}/file", server.uri()),
            &HashMap::new(),
            file.to_str(),
            &[dir.path().to_str().unwrap().into()],
        )
        .await
        .unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn malformed_instruction_keeps_transfer_error_context() {
        let batch = RecordBatch::try_from_iter(vec![(
            "operation",
            Arc::new(StringArray::from(vec!["GET"])) as arrow_array::ArrayRef,
        )])
        .unwrap();
        let error = complete(
            &client(),
            result(batch, true),
            &VolumeOptions {
                paths: Some(vec![]),
                retain_result: false,
            },
            &OnceCell::new(),
        )
        .await
        .err()
        .unwrap();
        assert_eq!(error.code, ErrorCode::InvalidArgument);
        assert_eq!(error.error_code.as_deref(), Some("FILE_TRANSFER_ERROR"));
        assert_eq!(error.query_id.as_deref(), Some("statement"));
        assert_eq!(error.sql_state.as_deref(), Some("HY000"));
    }

    #[tokio::test]
    async fn marker_and_opt_in_are_both_required() {
        let batch = instruction("GET", "http://127.0.0.1:1/file", "/unavailable");
        for (marker, options) in [
            (
                false,
                VolumeOptions {
                    paths: Some(vec![]),
                    retain_result: false,
                },
            ),
            (true, VolumeOptions::default()),
        ] {
            let completed = complete(
                &client(),
                result(batch.clone(), marker),
                &options,
                &OnceCell::new(),
            )
            .await
            .unwrap();
            let mut reader = ResultStream::from_reader(completed.reader).await.unwrap();
            assert!(reader.next_batch().await.unwrap().is_some());
        }
    }

    #[tokio::test]
    async fn denies_outside_paths_before_storage_request() {
        let dir = tempfile::tempdir().unwrap();
        let allowed = dir.path().join("allowed");
        std::fs::create_dir(&allowed).unwrap();
        let outside = dir.path().join("outside");
        let error = transfer(
            &client(),
            "GET",
            "http://127.0.0.1:1/file",
            &HashMap::new(),
            outside.to_str(),
            &[allowed.to_str().unwrap().into()],
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, ErrorCode::PermissionDenied);
        assert!(!outside.exists());
    }

    #[tokio::test]
    async fn download_body_uses_storage_deadline() {
        use crate::client::retry::{RequestCategory, RetryConfig};
        use std::time::Duration;
        use tokio::io::AsyncReadExt;
        for (budget, succeeds, existing) in
            [(1000, true, false), (40, false, true), (40, false, false)]
        {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    request.push(socket.read_u8().await.unwrap());
                }
                socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\n")
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(100)).await;
                let _ = socket.write_all(b"data").await;
            });
            let config = HttpClientConfig {
                read_timeout: Duration::from_millis(10),
                ..Default::default()
            };
            let retry = RetryConfig {
                overall_timeout: Duration::from_millis(budget),
                max_retries: 0,
                ..Default::default()
            };
            let http = DatabricksHttpClient::new(
                config,
                HashMap::from([(RequestCategory::CloudFetch, retry)]),
            )
            .unwrap();
            let dir = tempfile::tempdir().unwrap();
            let file = dir.path().join("download");
            if existing {
                std::fs::write(&file, b"original").unwrap();
            }
            let result = transfer(
                &http,
                "GET",
                &format!("http://{address}/file"),
                &HashMap::new(),
                file.to_str(),
                &[dir.path().to_str().unwrap().into()],
            )
            .await;
            assert_eq!(result.is_ok(), succeeds);
            if succeeds {
                assert_eq!(std::fs::read(&file).unwrap(), b"data");
            } else {
                assert_eq!(result.unwrap_err().code, ErrorCode::NetworkError);
                if existing {
                    assert_eq!(std::fs::read(&file).unwrap(), b"original");
                } else {
                    assert!(!file.exists());
                }
            }
            assert_eq!(
                std::fs::read_dir(dir.path()).unwrap().count(),
                usize::from(succeeds || existing)
            );
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn transport_cause_omits_signed_url() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let error = reqwest::Client::new()
            .get(format!("http://{address}/file?signature=secret"))
            .send()
            .await
            .unwrap_err();
        let error = transport_error(error);
        assert!(error.cause.is_some());
        assert!(!format!("{error:?}").contains("signature"));
        assert!(!format!("{error:?}").contains("secret"));
    }

    #[tokio::test]
    async fn rejects_plaintext_storage_host() {
        let error = transfer(
            &client(),
            "REMOVE",
            "http://storage.example/file?signature=secret",
            &HashMap::new(),
            None,
            &[],
        )
        .await
        .unwrap_err();
        assert!(error.message.contains("scheme"));
        assert!(!error.message.contains("secret"));
    }

    #[test]
    fn unresolvable_allowed_root_does_not_hide_later_root() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("download");
        let paths = vec![
            dir.path().join("missing/nested").to_str().unwrap().into(),
            dir.path().to_str().unwrap().into(),
        ];
        assert_eq!(allowed_path(file.to_str(), &paths).unwrap(), file);
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlinks_that_escape_allowed_directory() {
        let dir = tempfile::tempdir().unwrap();
        let allowed = dir.path().join("allowed");
        let outside = dir.path().join("outside");
        std::fs::create_dir(&allowed).unwrap();
        std::fs::create_dir(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, allowed.join("link")).unwrap();
        assert!(allowed_path(
            allowed.join("link/file").to_str(),
            &[allowed.to_str().unwrap().into()]
        )
        .is_err());
    }
}
