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
use crate::kernel_error::{Error, ErrorCode, OsError, Result};
use crate::reader::{EmptyReader, InMemoryReader};
use crate::result_stream::{ResultBatch, ResultStream};
use crate::statement::StatementSpec;
use arrow_array::{Array, MapArray, RecordBatch, StringArray};
use futures::StreamExt;
use reqwest::Method;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio_util::io::ReaderStream;

pub(crate) type VolumeCompletion = Mutex<Option<Result<()>>>;

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
    completion: &VolumeCompletion,
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
        let batch = match next_instruction_batch(&mut stream).await? {
            Some(batch) if batch.num_rows() == 1 => batch,
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
        if next_instruction_batch(&mut stream).await?.is_some() {
            return Err(Error::invalid_state().message("Expected one file transfer instruction"));
        }
        {
            let mut completed = completion.lock().await;
            if completed.is_none() {
                // Cancellation must leave a terminal result instead of replaying an upload.
                *completed = Some(Err(Error::new(
                    ErrorCode::Cancelled,
                    "File transfer interrupted; re-execute the SQL to retry",
                )));
                *completed = Some(
                    transfer(
                        client,
                        &operation,
                        &url,
                        &headers,
                        local_file.as_deref(),
                        paths,
                    )
                    .await
                    .map_err(|mut error| {
                        if let Some(os_error) = error.os_error.as_mut() {
                            os_error.filename = local_file.clone();
                        }
                        error
                    }),
                );
            }
            completed.as_ref().unwrap().clone()?;
        }
        result.reader = if options.retain_result {
            Box::new(InMemoryReader::new(schema, vec![batch]))
        } else {
            Box::new(EmptyReader::new(schema))
        };
        Ok(result)
    }
    .await
    .map_err(|mut e: Error| {
        if e.error_code.is_none() {
            e = e.with_error_code("FILE_TRANSFER_ERROR");
        }
        if e.sql_state.is_none() {
            let state = if e.code == ErrorCode::Timeout {
                "HYT00"
            } else {
                "HY000"
            };
            e = e.with_sql_state(state);
        }
        e.with_query_id(&statement_id)
    })
}

async fn next_instruction_batch(stream: &mut ResultStream) -> Result<Option<RecordBatch>> {
    loop {
        match stream.next_batch().await? {
            Some(ResultBatch::Arrow(batch)) if batch.num_rows() == 0 => continue,
            Some(ResultBatch::Arrow(batch)) => return Ok(Some(batch.clone())),
            None => return Ok(None),
        }
    }
}

fn instruction_headers(batch: &RecordBatch) -> Result<HashMap<String, String>> {
    let invalid = || {
        Error::invalid_state().message(format!(
            "Invalid file transfer headers: {:?}",
            batch.column(2).data_type()
        ))
    };
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
    let permission_denied = error.kind() == std::io::ErrorKind::PermissionDenied;
    let mut mapped = Error::new(
        match error.kind() {
            std::io::ErrorKind::NotFound => ErrorCode::NotFound,
            std::io::ErrorKind::PermissionDenied => ErrorCode::PermissionDenied,
            std::io::ErrorKind::InvalidInput => ErrorCode::InvalidArgument,
            std::io::ErrorKind::TimedOut => ErrorCode::Timeout,
            std::io::ErrorKind::StorageFull => ErrorCode::ResourceExhausted,
            _ => ErrorCode::Internal,
        },
        format!("File transfer failed: {error}"),
    );
    mapped.os_error = error.raw_os_error().map(|code| {
        Box::new(OsError {
            code,
            filename: None,
        })
    });
    if permission_denied {
        mapped.with_error_code("FILE_TRANSFER_PERMISSION_ERROR")
    } else {
        mapped
    }
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

fn missing_path_for_policy(path: &Path) -> Result<PathBuf> {
    let absolute = std::path::absolute(path).map_err(file_error)?;
    for ancestor in absolute.ancestors().skip(1) {
        if let Ok(mut resolved) = ancestor.canonicalize() {
            let suffix = absolute.strip_prefix(ancestor).unwrap();
            for component in suffix.components() {
                match component {
                    std::path::Component::ParentDir => {
                        resolved.pop();
                    }
                    std::path::Component::CurDir => {}
                    component => resolved.push(component.as_os_str()),
                }
            }
            return Ok(resolved);
        }
    }
    Err(Error::new(
        ErrorCode::PermissionDenied,
        "Cannot resolve local file path",
    ))
}

fn allowed_path(local_file: Option<&str>, paths: &[String]) -> Result<PathBuf> {
    let file = local_file.filter(|s| !s.is_empty()).ok_or_else(|| {
        Error::invalid_argument().message("Local file path is required for PUT and GET")
    })?;
    let resolved = resolved_path(Path::new(file));
    let policy_path = match &resolved {
        Ok(file) => file.clone(),
        Err(error) if error.code == ErrorCode::NotFound => {
            // Missing components may classify a denial, but must never authorize file access.
            missing_path_for_policy(Path::new(file))?
        }
        Err(error) => return Err(error.clone()),
    };
    for base in paths {
        let Ok(base) = resolved_path(Path::new(base)) else {
            continue;
        };
        if policy_path.starts_with(base) {
            return resolved;
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
    let operation = operation.to_ascii_uppercase();
    let method = match operation.as_str() {
        "PUT" => Method::PUT,
        "GET" => Method::GET,
        "REMOVE" => Method::DELETE,
        _ => return Err(Error::invalid_state().message("Unsupported file transfer operation")),
    };
    let parsed = reqwest::Url::parse(url)
        .map_err(|_| Error::invalid_state().message("Invalid file transfer URL"))?;
    if parsed.scheme() != "https"
        && !(client.allow_loopback_plaintext_storage
            && crate::client::http::is_loopback_http_url(&parsed))
    {
        return Err(Error::invalid_state().message("Invalid file transfer URL scheme"));
    }
    let local = if operation == "REMOVE" {
        None
    } else {
        Some(allowed_path(local_file, paths)?)
    };
    if operation == "GET" {
        let destination = local.as_ref().unwrap();
        if destination.parent().is_none() {
            return Err(Error::invalid_argument().message("Download destination must be a file"));
        }
        match tokio::fs::metadata(destination).await {
            Ok(metadata) if !metadata.is_file() => {
                return Err(
                    Error::invalid_argument().message("Download destination must be a file")
                );
            }
            Ok(_) => {
                tokio::fs::OpenOptions::new()
                    .write(true)
                    .open(destination)
                    .await
                    .map_err(file_error)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(file_error(error)),
        }
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
            let permissions = match std::fs::metadata(destination) {
                Ok(metadata) => metadata.permissions(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    std::fs::Permissions::from_mode(0o666)
                }
                Err(error) => return Err(file_error(error)),
            };
            builder.permissions(permissions);
            builder
        };
        let temporary = builder.tempfile_in(parent).map_err(file_error)?;
        let mut file =
            tokio::fs::File::from_std(temporary.as_file().try_clone().map_err(file_error)?);
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
    let code = if error.is_timeout() {
        ErrorCode::Timeout
    } else {
        ErrorCode::NetworkError
    };
    let message = if error.is_timeout() {
        "File transfer timed out"
    } else if error.is_connect() {
        "File transfer connection failed"
    } else if error.is_body() || error.is_decode() {
        "File download interrupted"
    } else {
        "File transfer request failed"
    };
    Error::new(code, message).with_cause(Error::new(code, format!("{:?}", error.without_url())))
}

async fn send_transfer(
    client: &DatabricksHttpClient,
    method: Method,
    url: reqwest::Url,
    headers: &HashMap<String, String>,
    local: Option<&Path>,
) -> Result<reqwest::Response> {
    let config = client.storage_retry_config();
    let request_type = if method == Method::PUT {
        RequestType::VolumeUpload
    } else {
        RequestType::CloudFetchDownload
    };
    let strategy = RetryStrategy::for_request(request_type, &config);
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
            .volume_client()?
            .request(method.clone(), url.clone())
            .timeout(remaining);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        if method == Method::PUT {
            let local = local
                .ok_or_else(|| Error::invalid_argument().message("Missing local upload path"))?;
            if !tokio::fs::metadata(local)
                .await
                .map_err(file_error)?
                .is_file()
            {
                return Err(Error::invalid_argument().message("Local upload path must be a file"));
            }
            // Each retry reopens the file so a streaming upload starts at byte zero.
            let file = tokio::fs::File::open(local).await.map_err(file_error)?;
            let metadata = file.metadata().await.map_err(file_error)?;
            if !metadata.is_file() {
                return Err(Error::invalid_argument().message("Local upload path must be a file"));
            }
            request = request
                .header(reqwest::header::CONTENT_LENGTH, metadata.len())
                .body(reqwest::Body::wrap_stream(ReaderStream::new(
                    file.take(metadata.len()),
                )));
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
        let mut client =
            DatabricksHttpClient::with_default_retry(HttpClientConfig::default()).unwrap();
        client.allow_loopback_plaintext_storage = true;
        client
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
            "gEt",
            &format!("{}/file", server.uri()),
            local.to_str().unwrap(),
        );
        let options = VolumeOptions {
            paths: Some(vec![dir.path().to_str().unwrap().into()]),
            retain_result: true,
        };
        let completion = VolumeCompletion::default();
        let http = client();
        for _ in 0..2 {
            let mut execution = result(batch.clone(), true);
            execution.reader = Box::new(InMemoryReader::new(
                batch.schema(),
                vec![
                    RecordBatch::new_empty(batch.schema()),
                    batch.clone(),
                    RecordBatch::new_empty(batch.schema()),
                ],
            ));
            let completed = complete(&http, execution, &options, &completion)
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

    #[tokio::test]
    async fn cancelled_reader_does_not_replay_upload() {
        use std::time::Duration;

        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(5)))
            .expect(1)
            .mount(&server)
            .await;
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("upload");
        std::fs::write(&source, b"upload").unwrap();
        let batch = instruction("PUT", &server.uri(), source.to_str().unwrap());
        let options = VolumeOptions {
            paths: Some(vec![directory.path().to_str().unwrap().into()]),
            retain_result: false,
        };
        let completion = VolumeCompletion::default();
        let http = client();
        let mut in_flight = Box::pin(complete(
            &http,
            result(batch.clone(), true),
            &options,
            &completion,
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            while server.received_requests().await.unwrap().is_empty() {
                tokio::select! {
                    _ = &mut in_flight => panic!("upload completed before cancellation"),
                    _ = tokio::time::sleep(Duration::from_millis(5)) => {}
                }
            }
        })
        .await
        .unwrap();
        drop(in_flight);
        let error = tokio::time::timeout(
            Duration::from_millis(500),
            complete(&http, result(batch, true), &options, &completion),
        )
        .await
        .unwrap()
        .err()
        .unwrap();
        assert_eq!(error.code, ErrorCode::Cancelled);
        assert_eq!(error.error_code.as_deref(), Some("FILE_TRANSFER_ERROR"));
        assert_eq!(error.query_id.as_deref(), Some("statement"));
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].body, b"upload");
    }

    #[test]
    fn file_errors_keep_their_category() {
        use std::io::ErrorKind;
        for (kind, code) in [
            (ErrorKind::NotFound, ErrorCode::NotFound),
            (ErrorKind::PermissionDenied, ErrorCode::PermissionDenied),
            (ErrorKind::InvalidInput, ErrorCode::InvalidArgument),
            (ErrorKind::StorageFull, ErrorCode::ResourceExhausted),
            (ErrorKind::TimedOut, ErrorCode::Timeout),
            (ErrorKind::IsADirectory, ErrorCode::Internal),
            (ErrorKind::ReadOnlyFilesystem, ErrorCode::Internal),
            (ErrorKind::Other, ErrorCode::Internal),
        ] {
            let error = file_error(std::io::Error::from(kind));
            assert_eq!(error.code, code);
            assert!(!error.retryable);
            assert!(error.os_error.is_none());
            assert_eq!(
                error.error_code.as_deref(),
                (kind == ErrorKind::PermissionDenied).then_some("FILE_TRANSFER_PERMISSION_ERROR")
            );
        }
        for raw in [1, 2, 5, 13] {
            let error = file_error(std::io::Error::from_raw_os_error(raw));
            assert_eq!(error.os_error.as_ref().map(|e| e.code), Some(raw));
        }
    }

    #[tokio::test]
    async fn local_os_errors_keep_requested_filename() {
        let server = MockServer::start().await;
        let dir = tempfile::tempdir().unwrap();
        let local = dir.path().join("absent-parent/file");
        let expected_code = std::fs::File::open(&local).unwrap_err().raw_os_error();
        let options = VolumeOptions {
            paths: Some(vec![dir.path().to_str().unwrap().into()]),
            retain_result: false,
        };
        for operation in ["PUT", "GET"] {
            let batch = instruction(operation, &server.uri(), local.to_str().unwrap());
            let error = complete(
                &client(),
                result(batch, true),
                &options,
                &VolumeCompletion::default(),
            )
            .await
            .err()
            .unwrap();
            assert_eq!(error.code, ErrorCode::NotFound);
            let os_error = error.os_error.as_ref().unwrap();
            assert_eq!(Some(os_error.code), expected_code);
            assert_eq!(os_error.filename.as_deref(), local.to_str());
            assert_eq!(error.error_code.as_deref(), Some("FILE_TRANSFER_ERROR"));
        }
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn storage_redirects_are_not_followed_and_plaintext_requires_opt_out() {
        let server = MockServer::start().await;
        Mock::given(path("/redirect"))
            .respond_with(
                ResponseTemplate::new(307)
                    .insert_header("Location", format!("{}/destination", server.uri())),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(path("/destination"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        let url = format!("{}/redirect", server.uri());
        let strict = DatabricksHttpClient::with_default_retry(HttpClientConfig::default()).unwrap();
        let error = transfer(&strict, "REMOVE", &url, &HashMap::new(), None, &[])
            .await
            .unwrap_err();
        assert!(error.message.contains("URL scheme"));
        let error = transfer(&client(), "REMOVE", &url, &HashMap::new(), None, &[])
            .await
            .unwrap_err();
        assert_eq!(error.http_status, Some(307));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn download_temporary_file_preserves_private_permissions() {
        use std::os::unix::fs::PermissionsExt;
        use std::time::Duration;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (resume, wait) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(socket.read_u8().await.unwrap());
            }
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\npriv")
                .await
                .unwrap();
            wait.await.unwrap();
            socket.write_all(b"ate\n").await.unwrap();
        });
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("download");
        std::fs::write(&destination, b"original").unwrap();
        std::fs::set_permissions(&destination, std::fs::Permissions::from_mode(0o600)).unwrap();
        let http = client();
        let url = format!("http://{address}/file");
        let headers = HashMap::new();
        let roots = vec![directory.path().to_str().unwrap().into()];
        let inspect = async {
            let permissions = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    for entry in std::fs::read_dir(directory.path()).unwrap() {
                        let entry = entry.unwrap();
                        if entry.path() != destination && entry.metadata().unwrap().len() > 0 {
                            return entry.metadata().unwrap().permissions().mode() & 0o777;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await;
            let original = std::fs::read(&destination).unwrap();
            resume.send(()).unwrap();
            (permissions.unwrap(), original)
        };
        let (result, (permissions, original)) = tokio::join!(
            transfer(&http, "GET", &url, &headers, destination.to_str(), &roots),
            inspect
        );
        result.unwrap();
        server.await.unwrap();
        assert_eq!(permissions, 0o600);
        assert_eq!(original, b"original");
        assert_eq!(std::fs::read(&destination).unwrap(), b"private\n");
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn download_preserves_file_permissions_and_rejects_root() {
        use std::os::unix::fs::PermissionsExt;
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"contents"))
            .expect(3)
            .mount(&server)
            .await;
        let dir = tempfile::tempdir().unwrap();
        let destination = dir.path().join("download");
        let control = dir.path().join("control");
        std::fs::write(&control, b"").unwrap();
        let roots = vec![dir.path().to_str().unwrap().into()];
        for existing in [None, Some(0o640), Some(0o200)] {
            if let Some(mode) = existing {
                std::fs::set_permissions(&destination, std::fs::Permissions::from_mode(mode))
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
            let expected = existing.unwrap_or_else(|| {
                std::fs::metadata(&control).unwrap().permissions().mode() & 0o777
            });
            assert_eq!(
                std::fs::metadata(&destination)
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                expected
            );
        }
        std::fs::write(&destination, b"protected").unwrap();
        std::fs::set_permissions(&destination, std::fs::Permissions::from_mode(0o440)).unwrap();
        if std::fs::OpenOptions::new()
            .write(true)
            .open(&destination)
            .is_err()
        {
            let error = transfer(
                &client(),
                "GET",
                &server.uri(),
                &HashMap::new(),
                destination.to_str(),
                &roots,
            )
            .await
            .unwrap_err();
            assert_eq!(error.code, ErrorCode::PermissionDenied);
            assert_eq!(
                error.error_code.as_deref(),
                Some("FILE_TRANSFER_PERMISSION_ERROR")
            );
            assert_eq!(std::fs::read(&destination).unwrap(), b"protected");
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
            "pUt",
            &format!("{}/file", server.uri()),
            &HashMap::new(),
            local.to_str(),
            &roots,
        )
        .await
        .unwrap();
        transfer(
            &http,
            "remove",
            &format!("{}/file", server.uri()),
            &HashMap::new(),
            None,
            &[],
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn rejects_fifo_before_opening_upload() {
        let server = MockServer::start().await;
        let directory = tempfile::tempdir().unwrap();
        let fifo = directory.path().join("input");
        assert!(std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success());
        let local = fifo.clone();
        let root = directory.path().to_str().unwrap().to_owned();
        let url = format!("{}/file", server.uri());
        let mut upload = tokio::spawn(async move {
            transfer(
                &client(),
                "PUT",
                &url,
                &HashMap::new(),
                local.to_str(),
                &[root],
            )
            .await
        });
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(1), &mut upload).await;
        if outcome.is_err() {
            // Unblock a regressed open so the runtime can shut down after the assertion.
            let _peer = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&fifo)
                .unwrap();
            let _ = upload.await;
            panic!("upload blocked while opening a FIFO");
        }
        assert_eq!(
            outcome.unwrap().unwrap().unwrap_err().code,
            ErrorCode::InvalidArgument
        );
        assert!(server.received_requests().await.unwrap().is_empty());
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
                .insert_header("Retry-After", "0")
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
    async fn does_not_retry_upload_after_response_is_lost() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/file", listener.local_addr().unwrap());
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&attempts);
        let server = tokio::spawn(async move {
            for attempt in 0..2 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut received = Vec::new();
                while !received.ends_with(b"\r\n\r\nupload") {
                    let mut buffer = [0u8; 1024];
                    let count = socket.read(&mut buffer).await.unwrap();
                    assert_ne!(count, 0);
                    received.extend_from_slice(&buffer[..count]);
                }
                observed.fetch_add(1, Ordering::SeqCst);
                if attempt == 1 {
                    socket
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                        .await
                        .unwrap();
                }
            }
        });
        let directory = tempfile::tempdir().unwrap();
        let local = directory.path().join("input");
        std::fs::write(&local, b"upload").unwrap();
        let outcome = transfer(
            &client(),
            "PUT",
            &url,
            &HashMap::new(),
            local.to_str(),
            &[directory.path().to_str().unwrap().into()],
        )
        .await;
        server.abort();
        assert!(outcome.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn missing_parent_preserves_policy_denial() {
        let directory = tempfile::tempdir().unwrap();
        let allowed = directory.path().join("allowed");
        std::fs::create_dir(&allowed).unwrap();
        let paths = [allowed.to_str().unwrap().to_owned()];
        for (path, expected) in [
            (allowed.join("missing/file"), ErrorCode::NotFound),
            (
                directory.path().join("outside/missing/file"),
                ErrorCode::PermissionDenied,
            ),
            (
                allowed.join("missing/../../outside/file"),
                ErrorCode::PermissionDenied,
            ),
        ] {
            let error = allowed_path(path.to_str(), &paths).unwrap_err();
            assert_eq!(error.code, expected);
        }
        #[cfg(unix)]
        {
            let outside = directory.path().join("outside");
            std::fs::create_dir(&outside).unwrap();
            std::os::unix::fs::symlink(&outside, allowed.join("link")).unwrap();
            let error =
                allowed_path(allowed.join("link/missing/file").to_str(), &paths).unwrap_err();
            assert_eq!(error.code, ErrorCode::PermissionDenied);
        }
    }

    #[tokio::test]
    async fn storage_deadline_keeps_timeout_state_and_transfer_context() {
        use crate::client::retry::{RequestCategory, RetryConfig};
        let server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        let retry = RetryConfig {
            overall_timeout: std::time::Duration::ZERO,
            ..Default::default()
        };
        let mut http = DatabricksHttpClient::new(
            HttpClientConfig::default(),
            HashMap::from([(RequestCategory::CloudFetch, retry)]),
        )
        .unwrap();
        http.allow_loopback_plaintext_storage = true;
        let batch = instruction("REMOVE", &server.uri(), "");
        let error = complete(
            &http,
            result(batch, true),
            &VolumeOptions {
                paths: Some(vec![]),
                retain_result: false,
            },
            &VolumeCompletion::default(),
        )
        .await
        .err()
        .unwrap();
        assert_eq!(error.code, ErrorCode::Timeout);
        assert_eq!(error.sql_state.as_deref(), Some("HYT00"));
        assert_eq!(error.error_code.as_deref(), Some("FILE_TRANSFER_ERROR"));
        assert_eq!(error.query_id.as_deref(), Some("statement"));
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
            &VolumeCompletion::default(),
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
    async fn instruction_read_failure_keeps_existing_error_context() {
        struct FailedReader;
        #[async_trait::async_trait]
        impl crate::reader::ResultReader for FailedReader {
            async fn schema(&self) -> Result<arrow_schema::SchemaRef> {
                Err(
                    Error::new(ErrorCode::NetworkError, "Instruction read failed")
                        .with_error_code("INSTRUCTION_READ_FAILED")
                        .with_sql_state("08S01"),
                )
            }
            async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
                unreachable!()
            }
        }
        let batch = instruction("GET", "https://storage.example/file", "/download");
        let mut execution = result(batch, true);
        execution.reader = Box::new(FailedReader);
        let error = complete(
            &client(),
            execution,
            &VolumeOptions {
                paths: Some(vec![]),
                retain_result: false,
            },
            &VolumeCompletion::default(),
        )
        .await
        .err()
        .unwrap();
        assert_eq!(error.error_code.as_deref(), Some("INSTRUCTION_READ_FAILED"));
        assert_eq!(error.sql_state.as_deref(), Some("08S01"));
        assert_eq!(error.query_id.as_deref(), Some("statement"));
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
                &VolumeCompletion::default(),
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
        for (budget, succeeds, existing) in [
            (10_000, true, false),
            (100, false, true),
            (100, false, false),
        ] {
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
                if succeeds {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    socket.write_all(b"data").await.unwrap();
                } else {
                    std::future::pending::<()>().await;
                }
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
            let mut http = DatabricksHttpClient::new(
                config,
                HashMap::from([(RequestCategory::CloudFetch, retry)]),
            )
            .unwrap();
            http.allow_loopback_plaintext_storage = true;
            let dir = tempfile::tempdir().unwrap();
            let file = dir.path().join("download");
            if existing {
                std::fs::write(&file, b"original").unwrap();
            }
            let batch = instruction(
                "GET",
                &format!("http://{address}/file"),
                file.to_str().unwrap(),
            );
            let result = complete(
                &http,
                result(batch, true),
                &VolumeOptions {
                    paths: Some(vec![dir.path().to_str().unwrap().into()]),
                    retain_result: false,
                },
                &VolumeCompletion::default(),
            )
            .await;
            assert_eq!(result.is_ok(), succeeds);
            if succeeds {
                assert_eq!(std::fs::read(&file).unwrap(), b"data");
            } else {
                let error = result.err().unwrap();
                assert_eq!(error.code, ErrorCode::Timeout);
                assert_eq!(error.sql_state.as_deref(), Some("HYT00"));
                assert_eq!(error.error_code.as_deref(), Some("FILE_TRANSFER_ERROR"));
                assert_eq!(error.query_id.as_deref(), Some("statement"));
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
            if succeeds {
                server.await.unwrap();
            } else {
                server.abort();
            }
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
