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

//! File transfers for Volume PUT, GET and REMOVE statements.
//!
//! The server answers these statements with one row holding a presigned
//! storage URL, request headers and a local path. This module makes that
//! storage request when the caller asks.

use crate::client::retry::{calculate_backoff, RequestType, RetryConfig, RetryStrategy};
use crate::kernel_error::{Error, LocalFileError, Result};
use arrow_array::{Array, LargeStringArray, MapArray, RecordBatch, StringArray, StringViewArray};
use futures::TryStreamExt;
use reqwest::{header::HeaderMap, Client, Method};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;
use tokio::sync::OnceCell;
use tokio_util::bytes::Bytes;
use tokio_util::io::ReaderStream;
use tokio_util::sync::CancellationToken;

/// Caller settings for a Volume file transfer.
#[non_exhaustive]
#[derive(Debug, Clone, Default)]
pub struct VolumeOptions {
    /// Local directories a PUT may read from and a GET may write to. Both the
    /// path named in the statement and the file it resolves to through
    /// symlinks must be inside one of them. Empty allows no local file access.
    /// The check runs before the file is opened, so nothing else may swap
    /// files or links in these directories during a transfer.
    pub allowed_paths: Vec<PathBuf>,
    /// Report success when REMOVE targets a file that does not exist.
    pub remove_not_found_is_success: bool,
}

impl VolumeOptions {
    pub fn new(allowed_paths: Vec<PathBuf>, remove_not_found_is_success: bool) -> Self {
        Self {
            allowed_paths,
            remove_not_found_is_success,
        }
    }
}

/// Outcome of `ResultStream::complete_volume_operation`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum VolumeCompletion {
    /// The result is not a Volume statement; nothing was done.
    NotVolume = 0,
    /// The PUT uploads caller-supplied bytes; call again with `input_bytes`.
    NeedsInputStream = 1,
    /// The transfer finished.
    Completed = 2,
}

pub(crate) struct VolumeState {
    retry: RetryConfig,
    storage: Arc<crate::client::http::StorageClient>,
    pub(crate) completion: OnceCell<Result<VolumeCompletion>>,
    cancellation: CancellationToken,
    started: AtomicBool,
}

fn cancelled() -> Error {
    Error::cancelled().message("Volume transfer was cancelled")
}

impl VolumeState {
    pub(crate) fn new(
        retry: RetryConfig,
        storage: Arc<crate::client::http::StorageClient>,
        cancellation: CancellationToken,
    ) -> Arc<Self> {
        Arc::new(Self {
            retry,
            storage,
            cancellation,
            completion: OnceCell::new(),
            started: AtomicBool::new(false),
        })
    }

    pub(crate) fn cancel(&self) {
        self.cancellation.cancel();
    }

    pub(crate) async fn complete(
        &self,
        prepare: impl std::future::Future<Output = Result<RecordBatch>>,
        options: &VolumeOptions,
        input_bytes: Option<Vec<u8>>,
    ) -> Result<VolumeCompletion> {
        match self
            .completion
            .get_or_try_init(|| async {
                // If an earlier call was dropped mid-transfer, do not run the transfer again.
                if self.started.swap(true, Ordering::AcqRel) || self.cancellation.is_cancelled() {
                    return Ok(Err(cancelled()));
                }
                let transfer = async {
                    let batch = prepare.await?;
                    let instruction = Instruction::decode(&batch)?;
                    if matches!(instruction.operation, Operation::Put(Source::Stream))
                        && input_bytes.is_none()
                    {
                        return Ok::<_, Error>((VolumeCompletion::NeedsInputStream, None));
                    }
                    let client = self.storage.get().await?;
                    let destination = self
                        .transfer(instruction, client, options, input_bytes)
                        .await?;
                    Ok((VolumeCompletion::Completed, destination))
                };
                let complete = async {
                    let (outcome, destination) = tokio::select! {
                        biased;
                        result = transfer => result?,
                        _ = self.cancellation.cancelled() => return Err(cancelled()),
                    };
                    if let Some((mut temp, existing, path, local_file)) = destination {
                        let cancellation = self.cancellation.clone();
                        tokio::task::spawn_blocking(move || {
                            if cancellation.is_cancelled() {
                                return Err(cancelled());
                            }
                            let mut file = match existing {
                                Some(file) => file,
                                None => std::fs::OpenOptions::new()
                                    .write(true)
                                    .create_new(true)
                                    .open(path)
                                    .map_err(|e| local_error(e, &local_file))?,
                            };
                            std::io::Seek::rewind(&mut temp)
                                .map_err(|e| local_error(e, &local_file))?;
                            let length = std::io::copy(&mut temp, &mut file)
                                .map_err(|e| local_error(e, &local_file))?;
                            file.set_len(length)
                                .map_err(|e| local_error(e, &local_file))
                        })
                        .await
                        .map_err(|e| {
                            Error::internal()
                                .message(format!("Volume download copy task failed: {e}"))
                        })??;
                    }
                    Ok(outcome)
                };
                let result = complete.await;
                if matches!(result, Ok(VolumeCompletion::NeedsInputStream)) {
                    self.started.store(false, Ordering::Release);
                    // Leave the cell uninitialized so completion can resume with input bytes.
                    Err(VolumeCompletion::NeedsInputStream)
                } else {
                    Ok(result)
                }
            })
            .await
        {
            Ok(result) => result.clone(),
            Err(outcome) => Ok(outcome),
        }
    }

    async fn transfer(
        &self,
        instruction: Instruction,
        client: &Client,
        options: &VolumeOptions,
        input_bytes: Option<Vec<u8>>,
    ) -> Result<Option<(std::fs::File, Option<std::fs::File>, PathBuf, String)>> {
        let input = if matches!(instruction.operation, Operation::Put(Source::Stream)) {
            Some(Bytes::from(
                input_bytes.ok_or_else(|| invalid("Missing input stream"))?,
            ))
        } else {
            None
        };
        let mut retry_started = None;
        let mut attempt = 0;
        loop {
            let mut retry_after = None;
            let error = match self
                .transfer_once(
                    &instruction,
                    client,
                    options,
                    input.as_ref(),
                    &mut retry_after,
                )
                .await
            {
                Ok(completion) => return Ok(completion),
                Err(error) => error,
            };
            // The retry clock starts at the first failure, so a long transfer does not
            // use up the retry budget.
            let started = retry_started.get_or_insert_with(std::time::Instant::now);
            if !error.retryable || attempt >= self.retry.max_retries {
                return Err(error.message(format!("after {} attempts", attempt + 1)));
            }
            let wait = calculate_backoff(&self.retry, attempt, retry_after.as_deref());
            if started.elapsed().saturating_add(wait) >= self.retry.overall_timeout {
                return Err(error.message(format!("after {} attempts", attempt + 1)));
            }
            tokio::time::sleep(wait).await;
            attempt += 1;
        }
    }

    async fn transfer_once(
        &self,
        instruction: &Instruction,
        client: &Client,
        options: &VolumeOptions,
        input: Option<&Bytes>,
        retry_after: &mut Option<String>,
    ) -> Result<Option<(std::fs::File, Option<std::fs::File>, PathBuf, String)>> {
        let operation = instruction.operation.clone();
        let roots = options.allowed_paths.clone();
        let preparation = tokio::task::spawn_blocking(move || {
            let mut source = None;
            let mut destination = None;
            let (method, local_file) = match operation {
                Operation::Put(Source::File(path)) => {
                    let local_file = path.to_string_lossy().into_owned();
                    let path = allowed_path(&local_file, &roots)?;
                    let mut file =
                        std::fs::File::open(path).map_err(|e| local_error(e, &local_file))?;
                    let metadata = file.metadata().map_err(|e| local_error(e, &local_file))?;
                    if metadata.is_dir() {
                        // A directory opens without error on Unix; reading it produces the OS error to report.
                        let error = std::io::Read::read(&mut file, &mut [0])
                            .err()
                            .unwrap_or_else(|| std::io::ErrorKind::IsADirectory.into());
                        return Err(local_error(error, &local_file));
                    }
                    source = Some((file, metadata.len()));
                    (Method::PUT, local_file)
                }
                Operation::Get(path) => {
                    let local_file = path.to_string_lossy().into_owned();
                    let path = allowed_path(&local_file, &roots)?;
                    let existing = match std::fs::OpenOptions::new().write(true).open(&path) {
                        Ok(file) => Some(file),
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                        Err(error) => return Err(local_error(error, &local_file)),
                    };
                    let temp = tempfile::tempfile_in(
                        path.parent()
                            .ok_or_else(|| invalid("Missing destination parent"))?,
                    )
                    .or_else(|_| tempfile::tempfile())
                    .map_err(|e| local_error(e, &local_file))?;
                    let writer = tokio::fs::File::from_std(
                        temp.try_clone().map_err(|e| local_error(e, &local_file))?,
                    );
                    destination = Some((temp, writer, existing, path));
                    (Method::GET, local_file)
                }
                Operation::Put(Source::Stream) => (Method::PUT, String::new()),
                Operation::Remove => (Method::DELETE, String::new()),
            };
            Ok((method, local_file, source, destination))
        });
        let (method, local_file, source, mut destination) = preparation.await.map_err(|e| {
            Error::internal().message(format!("Volume file preparation task failed: {e}"))
        })??;
        let upload = method == Method::PUT;
        let remove = method == Method::DELETE;
        let mut request = client
            .request(method, &instruction.url)
            .headers(instruction.headers.clone());
        let upload_error = Arc::new(Mutex::new(None));
        let (progress, changed) = tokio::sync::watch::channel(tokio::time::Instant::now());
        let reader: Option<(Box<dyn tokio::io::AsyncRead + Unpin + Send>, u64)> =
            if let Some((file, length)) = source {
                Some((Box::new(tokio::fs::File::from_std(file)), length))
            } else {
                input.map(|bytes| {
                    (
                        Box::new(std::io::Cursor::new(bytes.clone())) as _,
                        bytes.len() as u64,
                    )
                })
            };
        if let Some((reader, length)) = reader {
            request = request
                .headers(HeaderMap::from_iter([(
                    reqwest::header::CONTENT_LENGTH,
                    length.into(),
                )]))
                .body(upload_body(
                    reader,
                    local_file.clone(),
                    Arc::clone(&upload_error),
                    progress,
                ));
        }
        let mut response = storage_send(
            request,
            self.storage.read_timeout,
            upload,
            upload_error,
            changed,
        )
        .await?;
        if let Some(bytes) = input.filter(|_| matches!(response.status().as_u16(), 307 | 308)) {
            // reqwest cannot replay a streamed body on a redirect. Resend from memory
            // so it can follow; this resend has no idle timeout.
            response = client
                .put(&instruction.url)
                .headers(instruction.headers.clone())
                .headers(HeaderMap::from_iter([(
                    reqwest::header::CONTENT_LENGTH,
                    bytes.len().into(),
                )]))
                .body(reqwest::Body::from(bytes.clone()))
                .send()
                .await
                .map_err(storage_error)?;
        }
        let status = response.status();
        if !(status.is_success()
            || (remove && status.as_u16() == 404 && options.remove_not_found_is_success))
        {
            *retry_after = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let mut error = Error::from_http_status(status.as_u16())
                .with_error_code("VOLUME_STORAGE_HTTP_ERROR")
                .with_retryable(
                    RetryStrategy::for_request(RequestType::CloudFetchDownload, &self.retry)
                        .is_retryable_status(status, retry_after.is_some()),
                )
                .message(format!(
                    "Volume storage request failed: HTTP {}",
                    status.as_u16()
                ));
            let mut body = Vec::new();
            while body.len() < 4096 {
                match storage_read(self.storage.read_timeout, !upload, async {
                    response.chunk().await.map_err(response_body_error)
                })
                .await
                {
                    Ok(Some(chunk)) => {
                        body.extend_from_slice(&chunk[..chunk.len().min(4096 - body.len())])
                    }
                    _ => break,
                }
            }
            let text = String::from_utf8_lossy(&body);
            for tag in ["Code", "Message"] {
                if let Some(value) = text
                    .split_once(&format!("<{tag}>"))
                    .and_then(|(_, tail)| tail.split_once(&format!("</{tag}>")))
                    .map(|(value, _)| value)
                    .filter(|value| !value.contains(['<', '>']))
                {
                    error = error.message(format!("{tag}: {value}"));
                }
            }
            return Err(error);
        }
        while let Some(bytes) = storage_read(self.storage.read_timeout, !upload, async {
            response.chunk().await.map_err(response_body_error)
        })
        .await?
        {
            if let Some((_, writer, _, _)) = destination.as_mut() {
                writer
                    .write_all(&bytes)
                    .await
                    .map_err(|e| local_error(e, &local_file))?;
            }
        }
        if let Some((_, writer, _, _)) = destination.as_mut() {
            writer
                .flush()
                .await
                .map_err(|e| local_error(e, &local_file))?;
        }
        Ok(destination.map(|(temp, writer, existing, path)| {
            drop(writer);
            (temp, existing, path, local_file)
        }))
    }
}

fn upload_body(
    reader: impl tokio::io::AsyncRead + Unpin + Send + 'static,
    path: String,
    error: Arc<Mutex<Option<Error>>>,
    progress: tokio::sync::watch::Sender<tokio::time::Instant>,
) -> reqwest::Body {
    reqwest::Body::wrap_stream(
        ReaderStream::with_capacity(reader, 256 * 1024)
            .inspect_ok(move |_| {
                progress.send_replace(tokio::time::Instant::now());
            })
            .map_err(move |cause| {
                *error.lock().unwrap_or_else(|e| e.into_inner()) = Some(local_error(cause, &path));
                std::io::Error::other("Volume upload source could not be read")
            }),
    )
}

fn invalid(message: &str) -> Error {
    Error::invalid_argument().message(message)
}

async fn storage_send(
    request: reqwest::RequestBuilder,
    timeout: Option<std::time::Duration>,
    upload: bool,
    upload_error: Arc<Mutex<Option<Error>>>,
    mut progress: tokio::sync::watch::Receiver<tokio::time::Instant>,
) -> Result<reqwest::Response> {
    let send = async { request.send().await.map_err(storage_error) };
    tokio::pin!(send);
    let response = if let (true, Some(timeout)) = (upload, timeout) {
        loop {
            let last = *progress.borrow_and_update();
            let Some(deadline) = last.checked_add(timeout) else {
                break (&mut send).await;
            };
            tokio::select! {
                biased;
                response = &mut send => break response,
                _ = progress.changed(), if progress.has_changed().is_ok() => {}
                _ = tokio::time::sleep_until(deadline) => break Err(Error::network_error()
                    .with_error_code("VOLUME_STORAGE_NETWORK_ERROR")
                    .with_retryable(false).message("Volume storage upload or response wait timed out")),
            }
        }
    } else if upload {
        send.await
    } else {
        storage_read(timeout, true, send).await
    };
    if let Some(error) = upload_error
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .take()
    {
        return Err(error);
    }
    response
}

async fn storage_read<T>(
    timeout: Option<std::time::Duration>,
    retry_timeout: bool,
    read: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    match timeout {
        Some(timeout) => tokio::time::timeout(timeout, read)
            .await
            .unwrap_or_else(|_| {
                Err(Error::network_error()
                    .with_error_code("VOLUME_STORAGE_NETWORK_ERROR")
                    .with_retryable(retry_timeout)
                    .message("Volume storage read timed out"))
            }),
        None => read.await,
    }
}

fn storage_error(error: reqwest::Error) -> Error {
    let retryable =
        RetryStrategy::for_request(RequestType::CloudFetchDownload, &RetryConfig::default())
            .is_retryable_error(&error);
    let error = error.without_url();
    let mut causes = crate::client::error_chain(&error)
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    causes.dedup();
    Error::network_error()
        .with_error_code("VOLUME_STORAGE_NETWORK_ERROR")
        .with_retryable(retryable)
        .message(format!(
            "Volume storage request failed: {}",
            causes.join(": ")
        ))
}

fn response_body_error(error: reqwest::Error) -> Error {
    let retryable = error.is_body() || error.is_decode();
    let mut error = storage_error(error);
    error.retryable |= retryable;
    error
}

fn local_error(error: std::io::Error, path: &str) -> Error {
    match error.kind() {
        std::io::ErrorKind::NotFound => Error::not_found(),
        std::io::ErrorKind::PermissionDenied => Error::permission_denied(),
        std::io::ErrorKind::InvalidInput
        | std::io::ErrorKind::IsADirectory
        | std::io::ErrorKind::NotADirectory => Error::invalid_argument(),
        _ => Error::io(),
    }
    .with_error_code("VOLUME_LOCAL_FILE_ERROR")
    .with_sql_state("HY000")
    .message(format!("{error}: {path}"))
    .with_local_error(LocalFileError::new(
        error.raw_os_error(),
        error.kind(),
        path,
    ))
}

fn normalize(path: &Path) -> Result<PathBuf> {
    let absolute =
        std::path::absolute(path).map_err(|e| local_error(e, &path.to_string_lossy()))?;
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::ParentDir => {
                normalized.pop();
            }
            Component::CurDir => {}
            _ => normalized.push(component.as_os_str()),
        }
    }
    Ok(normalized)
}

const MAX_SYMLINK_RESOLUTIONS: u8 = 40;

fn resolve_existing_parent(
    path: &Path,
    requested: &str,
    remaining_symlinks: u8,
) -> Result<PathBuf> {
    match path.canonicalize() {
        Ok(path) => Ok(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink()) {
                if remaining_symlinks == 0 {
                    return Err(local_error(error, requested));
                }
                let target = std::fs::read_link(path).map_err(|e| local_error(e, requested))?;
                let target = if target.is_absolute() {
                    target
                } else {
                    path.parent()
                        .ok_or_else(|| invalid("Invalid local path"))?
                        .join(target)
                };
                return resolve_existing_parent(
                    &normalize(&target)?,
                    requested,
                    remaining_symlinks - 1,
                );
            }
            let parent = path.parent().ok_or_else(|| local_error(error, requested))?;
            let name = path
                .file_name()
                .ok_or_else(|| invalid("Invalid local path"))?;
            Ok(resolve_existing_parent(parent, requested, remaining_symlinks)?.join(name))
        }
        Err(error) => Err(local_error(error, requested)),
    }
}

/// Whether `path` is `root` or inside it. The drive or share prefix never
/// depends on case. After resolving links, pass `ignore_name_case: false`:
/// a case-sensitive directory can hold two names that differ only by case.
#[cfg(windows)]
fn within_root(path: &Path, root: &Path, ignore_name_case: bool) -> bool {
    let lower = |part: Component<'_>| part.as_os_str().to_string_lossy().to_lowercase();
    let mut path = path.components();
    root.components().all(|root| {
        path.next().is_some_and(|part| {
            if ignore_name_case || matches!(root, Component::Prefix(_)) {
                lower(part) == lower(root)
            } else {
                part == root
            }
        })
    })
}

#[cfg(not(windows))]
fn within_root(path: &Path, root: &Path, _ignore_name_case: bool) -> bool {
    path.starts_with(root)
}

fn allowed_path(requested: &str, roots: &[PathBuf]) -> Result<PathBuf> {
    if requested.is_empty() {
        return Err(invalid("Local file path is empty"));
    }
    let path = normalize(Path::new(requested))?;
    let roots = roots
        .iter()
        .filter(|root| !root.as_os_str().is_empty())
        .map(|root| normalize(root))
        .collect::<Result<Vec<_>>>()?;
    if !roots.iter().any(|root| within_root(&path, root, true)) {
        return Err(
            invalid("Local file path is outside the configured allowed roots")
                .with_error_code("VOLUME_PATH_NOT_ALLOWED"),
        );
    }
    let resolved = resolve_existing_parent(&path, requested, MAX_SYMLINK_RESOLUTIONS)?;
    let mut root_error = None;
    for root in roots {
        let root = match resolve_existing_parent(
            &root,
            &root.to_string_lossy(),
            MAX_SYMLINK_RESOLUTIONS,
        ) {
            Ok(root) => root,
            Err(error) => {
                root_error.get_or_insert(error);
                continue;
            }
        };
        if within_root(&resolved, &root, false) {
            return Ok(resolved);
        }
    }
    if let Some(error) = root_error {
        return Err(error);
    }
    Err(
        invalid("Resolved local file path is outside the configured allowed roots")
            .with_error_code("VOLUME_PATH_NOT_ALLOWED"),
    )
}

#[derive(Clone)]
enum Source {
    File(PathBuf),
    Stream,
}

#[derive(Clone)]
enum Operation {
    Put(Source),
    Get(PathBuf),
    Remove,
}

struct Instruction {
    operation: Operation,
    url: String,
    headers: HeaderMap,
}

fn string_at(array: &dyn Array, row: usize) -> Result<&str> {
    if array.is_null(row) {
        return Err(invalid("Null Volume instruction field"));
    }
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(values.value(row));
    }
    if let Some(values) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(values.value(row));
    }
    if let Some(values) = array.as_any().downcast_ref::<StringViewArray>() {
        return Ok(values.value(row));
    }
    Err(invalid("Invalid Volume instruction column type"))
}

impl Instruction {
    fn decode(batch: &RecordBatch) -> Result<Self> {
        if batch.num_rows() != 1 {
            return Err(invalid("Expected one Volume instruction row"));
        }
        let schema = batch.schema();
        let column = |name: &str| -> Result<&dyn Array> {
            let index = schema
                .fields()
                .iter()
                .position(|f| f.name() == name)
                .ok_or_else(|| invalid("Missing Volume instruction column"))?;
            Ok(batch.column(index).as_ref())
        };
        let operation = string_at(column("operation")?, 0)?;
        let headers = column("headers")?;
        let mut parsed_headers = HeaderMap::new();
        let mut add_header = |name: &str, value: &str| -> Result<()> {
            parsed_headers.insert(
                reqwest::header::HeaderName::from_bytes(name.as_bytes())
                    .map_err(|_| invalid("Invalid Volume storage header name"))?,
                value
                    .parse()
                    .map_err(|_| invalid("Invalid Volume storage header value"))?,
            );
            Ok(())
        };
        if !headers.is_null(0) {
            if let Some(map) = headers.as_any().downcast_ref::<MapArray>() {
                let entries = map.value(0);
                for row in 0..entries.len() {
                    add_header(
                        string_at(entries.column(0).as_ref(), row)?,
                        string_at(entries.column(1).as_ref(), row)?,
                    )?;
                }
            } else {
                let headers: std::collections::HashMap<String, String> =
                    serde_json::from_str(string_at(headers, 0)?)
                        .map_err(|_| invalid("Invalid Volume storage headers"))?;
                for (name, value) in headers {
                    add_header(&name, &value)?;
                }
            }
        }
        let operation = match operation {
            "PUT" => match string_at(column("localFile")?, 0)? {
                "__input_stream__" => Operation::Put(Source::Stream),
                path => Operation::Put(Source::File(path.into())),
            },
            "GET" => Operation::Get(string_at(column("localFile")?, 0)?.into()),
            "REMOVE" => Operation::Remove,
            _ => return Err(invalid("Unsupported Volume operation")),
        };
        Ok(Self {
            operation,
            url: string_at(column("presignedUrl")?, 0)?.to_owned(),
            headers: parsed_headers,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{MapBuilder, StringBuilder};
    use arrow_array::{ArrayRef, RecordBatch};
    use arrow_schema::{Field, Schema};
    #[cfg(unix)]
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    use std::time::Duration;
    use wiremock::matchers::{body_bytes, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn instruction(operation: &str, url: &str, local: &Path) -> RecordBatch {
        let mut headers = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        headers.keys().append_value("x-storage-token");
        headers.values().append_value("server-token");
        headers.append(true).unwrap();
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![operation])),
            Arc::new(StringArray::from(vec![url])),
            Arc::new(headers.finish()),
            Arc::new(StringArray::from(vec![local.to_str().unwrap()])),
        ];
        let schema = Schema::new(
            ["operation", "presignedUrl", "headers", "localFile"]
                .iter()
                .zip(&arrays)
                .map(|(name, values)| Field::new(*name, values.data_type().clone(), true))
                .collect::<Vec<_>>(),
        );
        RecordBatch::try_new(Arc::new(schema), arrays).unwrap()
    }

    fn options(root: &Path) -> VolumeOptions {
        VolumeOptions {
            allowed_paths: vec![root.to_owned()],
            ..Default::default()
        }
    }

    fn storage(read_timeout: Option<Duration>) -> Arc<crate::client::http::StorageClient> {
        Arc::new(
            crate::client::DatabricksHttpClient::with_default_retry(Default::default())
                .unwrap()
                .storage_client(read_timeout),
        )
    }

    fn transfer_state(max_retries: u32) -> Arc<VolumeState> {
        VolumeState::new(
            RetryConfig {
                max_retries,
                min_wait: Duration::ZERO,
                max_wait: Duration::ZERO,
                ..Default::default()
            },
            storage(None),
            Default::default(),
        )
    }

    #[tokio::test]
    async fn local_upload_read_errors_wrapped_as_request_errors_are_not_retried() {
        let server = MockServer::start().await;
        struct FailedRead(Option<std::io::Error>);
        impl tokio::io::AsyncRead for FailedRead {
            fn poll_read(
                mut self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
                _: &mut tokio::io::ReadBuf<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Ready(Err(self.0.take().unwrap()))
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("missing");
        let cause = std::fs::File::open(&source).unwrap_err();
        let os_code = cause.raw_os_error();
        let captured = Arc::new(Mutex::new(None));
        let body = upload_body(
            FailedRead(Some(cause)),
            source.to_str().unwrap().into(),
            Arc::clone(&captured),
            tokio::sync::watch::channel(tokio::time::Instant::now()).0,
        );
        let error = storage_send(
            Client::new()
                .put(format!("{}/object?secret=hidden", server.uri()))
                .body(body),
            None,
            true,
            captured,
            tokio::sync::watch::channel(tokio::time::Instant::now()).1,
        )
        .await
        .unwrap_err();
        assert!(!error.retryable);
        assert_eq!(error.code, crate::ErrorCode::NotFound);
        assert_eq!(error.error_code.as_deref(), Some("VOLUME_LOCAL_FILE_ERROR"));
        let local = error.local_error.unwrap();
        assert_eq!(local.raw_os_error, os_code);
        assert_eq!(local.path, source.to_str().unwrap());
        assert!(!error.message.contains("hidden"));
    }

    #[tokio::test]
    async fn transient_storage_statuses_replay_full_inputs_and_cache_completion() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        for (operation, stream) in [("PUT", false), ("PUT", true), ("GET", false)] {
            let requests = 2;
            let server = MockServer::start().await;
            let attempts = Arc::new(AtomicUsize::new(0));
            let count = Arc::clone(&attempts);
            Mock::given(method(operation))
                .and(path("/object"))
                .and(header("x-storage-token", "server-token"))
                .respond_with(move |request: &wiremock::Request| {
                    if operation == "PUT" {
                        assert_eq!(request.body, b"payload");
                    }
                    if count.fetch_add(1, Ordering::SeqCst) == 0 {
                        ResponseTemplate::new(503)
                            .insert_header("retry-after", "0")
                            .set_delay(Duration::from_millis(200))
                    } else {
                        ResponseTemplate::new(200).set_body_bytes(b"payload")
                    }
                })
                .expect(requests)
                .mount(&server)
                .await;
            let dir = tempfile::tempdir().unwrap();
            let file = dir.path().join("file");
            std::fs::write(
                &file,
                if operation == "GET" {
                    b"old".as_slice()
                } else {
                    b"payload"
                },
            )
            .unwrap();
            let local = if stream {
                Path::new("__input_stream__")
            } else {
                &file
            };
            let batch = instruction(operation, &format!("{}/object", server.uri()), local);
            let mut state = transfer_state(6);
            Arc::get_mut(&mut state).unwrap().retry.overall_timeout = Duration::from_millis(100);
            let options = options(dir.path());
            assert_eq!(
                state
                    .complete(
                        async { Ok(batch.clone()) },
                        &options,
                        stream.then(|| b"payload".to_vec())
                    )
                    .await
                    .unwrap(),
                VolumeCompletion::Completed
            );
            assert_eq!(
                state
                    .complete(async { Ok(batch.clone()) }, &options, None)
                    .await
                    .unwrap(),
                VolumeCompletion::Completed
            );
            assert_eq!(attempts.load(Ordering::SeqCst), requests as usize);
            assert_eq!(std::fs::read(file).unwrap(), b"payload");
            assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
        }
    }

    #[tokio::test]
    async fn storage_statuses_and_retry_limits_preserve_classification() {
        for (operation, status, retries, budget, requests, missing_success) in [
            ("PUT", 307, 2, Duration::from_secs(10), 1, true),
            ("REMOVE", 403, 2, Duration::from_secs(10), 1, true),
            ("REMOVE", 404, 2, Duration::from_secs(10), 1, true),
            ("REMOVE", 404, 2, Duration::from_secs(10), 1, false),
            ("REMOVE", 503, 0, Duration::from_secs(10), 1, true),
            ("REMOVE", 503, 2, Duration::ZERO, 1, true),
            ("REMOVE", 503, 2, Duration::from_secs(10), 3, true),
        ] {
            let server = MockServer::start().await;
            Mock::given(method(if operation == "PUT" { "PUT" } else { "DELETE" }))
                .and(path("/object"))
                .respond_with(
                    ResponseTemplate::new(status)
                        .insert_header("retry-after", "0")
                        .insert_header("location", "/redirected")
                        .set_body_string("<Error><Code>AccessDenied</Code><Message>Permission refused</Message><SignatureProvided>secret</SignatureProvided></Error>"),
                )
                .expect(requests)
                .mount(&server)
                .await;
            let directory = tempfile::tempdir().unwrap();
            let file = directory.path().join("source");
            std::fs::write(&file, b"payload").unwrap();
            let options = VolumeOptions {
                allowed_paths: vec![directory.path().to_owned()],
                remove_not_found_is_success: missing_success,
            };
            let state = VolumeState::new(
                RetryConfig {
                    max_retries: retries,
                    overall_timeout: budget,
                    min_wait: Duration::ZERO,
                    max_wait: Duration::ZERO,
                    ..Default::default()
                },
                storage(None),
                Default::default(),
            );
            let batch = instruction(
                operation,
                &format!("{}/object?secret=hidden", server.uri()),
                &file,
            );
            for _ in 0..2 {
                let result = state
                    .complete(async { Ok(batch.clone()) }, &options, None)
                    .await;
                if status == 404 && missing_success {
                    assert_eq!(result.unwrap(), VolumeCompletion::Completed);
                    continue;
                }
                let error = result.unwrap_err();
                assert_eq!(error.http_status, Some(status));
                assert_eq!(
                    error.error_code.as_deref(),
                    Some("VOLUME_STORAGE_HTTP_ERROR")
                );
                assert_eq!(error.retryable, status == 503);
                assert!(error.message.contains("AccessDenied"));
                assert!(error
                    .message
                    .contains(&format!("after {requests} attempts")));
                assert!(!error.message.contains("secret"));
            }
        }
    }

    #[tokio::test]
    async fn cancellation_interrupts_retry_backoff_without_another_request() {
        let server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .respond_with(ResponseTemplate::new(503).insert_header("retry-after", "30"))
            .expect(1)
            .mount(&server)
            .await;
        let state = VolumeState::new(
            RetryConfig {
                min_wait: Duration::from_secs(30),
                max_wait: Duration::from_secs(30),
                ..Default::default()
            },
            storage(None),
            Default::default(),
        );
        let batch = instruction("REMOVE", &server.uri(), Path::new(""));
        let options = VolumeOptions::default();
        let complete = state.complete(async { Ok(batch.clone()) }, &options, None);
        let cancel = async {
            while server.received_requests().await.unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            state.cancel();
        };
        let (result, _) = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(complete, cancel)
        })
        .await
        .unwrap();
        assert_eq!(result.unwrap_err().code, crate::ErrorCode::Cancelled);
    }

    #[tokio::test]
    async fn stream_handshake_accepts_empty_bytes_once_and_keeps_success_after_cancel() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/object"))
            .and(body_bytes(Vec::<u8>::new()))
            .and(header("content-length", "0"))
            .and(header("x-storage-token", "server-token"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        let batch = instruction(
            "PUT",
            &format!("{}/object", server.uri()),
            Path::new("__input_stream__"),
        );
        let state = transfer_state(0);
        assert_eq!(
            state
                .complete(async { Ok(batch.clone()) }, &VolumeOptions::default(), None)
                .await
                .unwrap(),
            VolumeCompletion::NeedsInputStream
        );
        assert!(server.received_requests().await.unwrap().is_empty());
        assert_eq!(
            state
                .complete(
                    async { Ok(batch.clone()) },
                    &VolumeOptions::default(),
                    Some(vec![])
                )
                .await
                .unwrap(),
            VolumeCompletion::Completed
        );
        state.cancel();
        assert_eq!(
            state
                .complete(
                    async { Ok(batch.clone()) },
                    &VolumeOptions::default(),
                    Some(vec![1])
                )
                .await
                .unwrap(),
            VolumeCompletion::Completed
        );
    }

    #[tokio::test]
    async fn get_writes_in_place_and_preserves_old_bytes_on_download_failure() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/ok"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"new bytes"))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/fail"))
            .respond_with(ResponseTemplate::new(403))
            .expect(1)
            .mount(&server)
            .await;
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("download");
        std::fs::write(&dest, b"old bytes").unwrap();
        #[cfg(unix)]
        let inode = std::fs::metadata(&dest).unwrap().ino();
        #[cfg(unix)]
        std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(0o200)).unwrap();
        let batch = instruction("GET", &format!("{}/ok", server.uri()), &dest);
        let completed = transfer_state(0);
        completed
            .complete(async { Ok(batch.clone()) }, &options(dir.path()), None)
            .await
            .unwrap();
        #[cfg(unix)]
        {
            assert_eq!(std::fs::metadata(&dest).unwrap().ino(), inode);
            assert_eq!(
                std::fs::metadata(&dest).unwrap().permissions().mode() & 0o777,
                0o200
            );
            std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        assert_eq!(std::fs::read(&dest).unwrap(), b"new bytes");
        #[cfg(unix)]
        {
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o500)).unwrap();
            if tempfile::tempfile_in(dir.path())
                .is_err_and(|error| error.kind() == std::io::ErrorKind::PermissionDenied)
            {
                std::fs::write(&dest, b"old bytes with a longer tail").unwrap();
                std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(0o200)).unwrap();
                Mock::given(method("GET"))
                    .and(path("/in-place"))
                    .respond_with(ResponseTemplate::new(200).set_body_bytes(b"new bytes"))
                    .expect(1)
                    .mount(&server)
                    .await;
                let batch = instruction("GET", &format!("{}/in-place", server.uri()), &dest);
                let state = transfer_state(0);
                assert_eq!(
                    state
                        .complete(async { Ok(batch.clone()) }, &options(dir.path()), None)
                        .await
                        .unwrap(),
                    VolumeCompletion::Completed
                );
                assert_eq!(
                    std::fs::metadata(&dest).unwrap().permissions().mode() & 0o777,
                    0o200
                );
                assert_eq!(std::fs::metadata(&dest).unwrap().ino(), inode);
                std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(0o600)).unwrap();
                assert_eq!(std::fs::read(&dest).unwrap(), b"new bytes");
            } else {
                eprintln!("SKIP: restricted-directory permissions are not enforced for this user");
            }
        }
        let state = transfer_state(0);
        let batch = instruction(
            "GET",
            &format!("{}/fail?secret=hidden", server.uri()),
            &dest,
        );
        let error = state
            .complete(async { Ok(batch.clone()) }, &options(dir.path()), None)
            .await
            .unwrap_err();
        assert_eq!(error.http_status, Some(403));
        assert!(!error.message.contains("hidden"));
        assert_eq!(std::fs::read(&dest).unwrap(), b"new bytes");
        #[cfg(unix)]
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn cancellation_returns_while_local_preparation_waits_for_a_fifo_writer() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        assert!(std::process::Command::new("mkfifo")
            .arg(&source)
            .status()
            .unwrap()
            .success());
        let state = transfer_state(0);
        let canceller = Arc::clone(&state);
        let batch = instruction("PUT", "http://unused.invalid/object", &source);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            canceller.cancel();
        });
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            state.complete(async { Ok(batch) }, &options(dir.path()), None),
        )
        .await;
        let _writer = std::fs::OpenOptions::new()
            .write(true)
            .open(source)
            .unwrap();
        assert_eq!(
            result.unwrap().unwrap_err().code,
            crate::ErrorCode::Cancelled
        );
    }

    #[tokio::test]
    async fn abandoned_or_cancelled_get_cannot_publish_or_restart() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(b"new")
                    .set_delay(Duration::from_millis(300)),
            )
            .expect(2)
            .mount(&server)
            .await;
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("download");
        std::fs::write(&dest, b"old").unwrap();
        let batch = instruction("GET", &server.uri(), &dest);
        let options = options(dir.path());
        for cancel in [false, true] {
            let state = transfer_state(0);
            let mut future = Box::pin(state.complete(async { Ok(batch.clone()) }, &options, None));
            tokio::select! {
                result = &mut future => panic!("unexpected early completion: {result:?}"),
                _ = async {
                    loop {
                        if server.received_requests().await.unwrap().len() >= if cancel { 2 } else { 1 } { break; }
                        tokio::task::yield_now().await;
                    }
                } => {}
            }
            if cancel {
                state.cancel();
                assert_eq!(
                    future.as_mut().await.unwrap_err().code,
                    crate::ErrorCode::Cancelled
                );
            }
            drop(future);
            assert_eq!(
                state
                    .complete(async { Ok(batch.clone()) }, &options, None)
                    .await
                    .unwrap_err()
                    .code,
                crate::ErrorCode::Cancelled
            );
            assert_eq!(std::fs::read(&dest).unwrap(), b"old");
        }
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn response_inactivity_bounds_headers_and_body_without_publishing() {
        use tokio::io::AsyncReadExt;
        for (operation, headers_sent) in [("GET", false), ("GET", true), ("PUT", true)] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let received = Arc::clone(&requests);
            let peer = tokio::spawn(async move {
                loop {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    received.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    tokio::spawn(async move {
                        let mut request = Vec::new();
                        let mut byte = [0];
                        while !request.ends_with(b"\r\n\r\n") {
                            socket.read_exact(&mut byte).await.unwrap();
                            request.push(byte[0]);
                        }
                        let size = String::from_utf8(request)
                            .unwrap()
                            .lines()
                            .find_map(|line| {
                                line.to_lowercase()
                                    .strip_prefix("content-length:")
                                    .map(|size| size.trim().parse::<usize>().unwrap())
                            })
                            .unwrap_or(0);
                        socket.read_exact(&mut vec![0; size]).await.unwrap();
                        if headers_sent {
                            socket
                                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 20\r\n\r\nshort")
                                .await
                                .unwrap();
                        }
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    });
                }
            });
            let dir = tempfile::tempdir().unwrap();
            let dest = dir.path().join("download");
            std::fs::write(&dest, b"old").unwrap();
            let state = VolumeState::new(
                transfer_state(if operation == "PUT" { 1 } else { 0 })
                    .retry
                    .clone(),
                storage(Some(Duration::from_millis(150))),
                Default::default(),
            );
            let batch = instruction(operation, &url, &dest);
            let error = tokio::time::timeout(
                Duration::from_secs(2),
                state.complete(async { Ok(batch.clone()) }, &options(dir.path()), None),
            )
            .await
            .unwrap()
            .unwrap_err();
            assert_eq!(error.code, crate::ErrorCode::NetworkError);
            assert_eq!(
                error.error_code.as_deref(),
                Some("VOLUME_STORAGE_NETWORK_ERROR")
            );
            assert!(error.message.contains("read timed out"));
            let repeated = state
                .complete(async { Ok(batch.clone()) }, &options(dir.path()), None)
                .await
                .unwrap_err();
            assert_eq!(repeated.message, error.message);
            assert_eq!(requests.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert_eq!(std::fs::read(&dest).unwrap(), b"old");
            assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
            peer.abort();
        }
    }

    #[tokio::test]
    async fn upload_accepts_maximum_idle_timeout() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(body_bytes(b"upload"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        let state = VolumeState::new(
            transfer_state(0).retry.clone(),
            storage(Some(Duration::MAX)),
            Default::default(),
        );
        assert_eq!(
            state
                .complete(
                    async {
                        Ok(instruction(
                            "PUT",
                            &server.uri(),
                            Path::new("__input_stream__"),
                        ))
                    },
                    &VolumeOptions::default(),
                    Some(b"upload".to_vec()),
                )
                .await
                .unwrap(),
            VolumeCompletion::Completed
        );
    }

    #[tokio::test]
    async fn get_progress_and_put_idle_limits() {
        use tokio::io::AsyncReadExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 2048];
            assert!(socket.read(&mut request).await.unwrap() > 0);
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n")
                .await
                .unwrap();
            for _ in 0..5 {
                tokio::time::sleep(Duration::from_millis(60)).await;
                socket.write_all(b"x").await.unwrap();
            }
        });
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("download");
        let state = VolumeState::new(
            transfer_state(0).retry.clone(),
            storage(Some(Duration::from_millis(150))),
            Default::default(),
        );
        state
            .complete(
                async { Ok(instruction("GET", &url, &dest)) },
                &options(dir.path()),
                None,
            )
            .await
            .unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), b"xxxxx");
        peer.await.unwrap();

        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(body_bytes(b"xxxxx"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        let (mut writer, reader) = tokio::io::duplex(1);
        let producer = tokio::spawn(async move {
            for _ in 0..5 {
                tokio::time::sleep(Duration::from_millis(60)).await;
                writer.write_all(b"x").await.unwrap();
            }
        });
        let error = Arc::new(Mutex::new(None));
        let (progress, changed) = tokio::sync::watch::channel(tokio::time::Instant::now());
        let body = upload_body(reader, "input".into(), Arc::clone(&error), progress);
        storage_send(
            Client::new()
                .put(server.uri())
                .header(reqwest::header::CONTENT_LENGTH, 5)
                .body(body),
            Some(Duration::from_millis(150)),
            true,
            error,
            changed,
        )
        .await
        .unwrap();
        producer.await.unwrap();

        for header_wait in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let payload = vec![b'x'; 16 * 1024 * 1024];
            std::fs::write(&dest, &payload).unwrap();
            let length = payload.len();
            let peer = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                if header_wait {
                    let mut headers = Vec::new();
                    while !headers.ends_with(b"\r\n\r\n") {
                        headers.push(socket.read_u8().await.unwrap());
                    }
                    let mut remaining = length;
                    let mut buffer = vec![0; 256 * 1024];
                    while remaining > 0 {
                        let n = socket.read(&mut buffer).await.unwrap();
                        assert!(n > 0);
                        remaining -= n;
                    }
                }
                std::future::pending::<()>().await;
            });
            let state = VolumeState::new(
                transfer_state(0).retry.clone(),
                storage(Some(Duration::from_millis(150))),
                Default::default(),
            );
            let error = tokio::time::timeout(
                Duration::from_secs(3),
                state.complete(
                    async { Ok(instruction("PUT", &url, &dest)) },
                    &options(dir.path()),
                    None,
                ),
            )
            .await
            .expect("PUT idle watchdog")
            .unwrap_err();
            assert_eq!(
                error.error_code.as_deref(),
                Some("VOLUME_STORAGE_NETWORK_ERROR")
            );
            assert!(error.message.contains("timed out"));
            peer.abort();
        }
    }

    #[tokio::test]
    async fn truncated_get_retries_from_clean_temp_and_preserves_destination_on_failure() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for retries in [0, 1] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let peer = tokio::spawn(async move {
                for attempt in 0..=retries {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let mut request = [0; 2048];
                    assert!(socket.read(&mut request).await.unwrap() > 0);
                    let response = if attempt == 0 {
                        b"HTTP/1.1 200 OK\r\nContent-Length: 20\r\nConnection: close\r\n\r\nshort"
                            .as_slice()
                    } else {
                        b"HTTP/1.1 200 OK\r\nContent-Length: 7\r\nConnection: close\r\n\r\ncorrect"
                    };
                    socket.write_all(response).await.unwrap();
                }
            });
            let dir = tempfile::tempdir().unwrap();
            let dest = dir.path().join("download");
            std::fs::write(&dest, b"old").unwrap();
            let batch = instruction("GET", &url, &dest);
            let result = transfer_state(retries)
                .complete(async { Ok(batch.clone()) }, &options(dir.path()), None)
                .await;
            if retries == 0 {
                assert_eq!(result.unwrap_err().code, crate::ErrorCode::NetworkError);
                assert_eq!(std::fs::read(&dest).unwrap(), b"old");
            } else {
                assert_eq!(result.unwrap(), VolumeCompletion::Completed);
                assert_eq!(std::fs::read(&dest).unwrap(), b"correct");
            }
            assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
            peer.await.unwrap();
        }
    }

    #[cfg(windows)]
    #[test]
    fn resolved_names_match_exactly_and_prefix_ignores_case() {
        for (path, root, allowed) in [
            (r"\\?\C:\data\safe\x", r"\\?\C:\data\safe", true),
            (r"\\?\c:\data\safe\x", r"\\?\C:\data\safe", true),
            (r"\\?\C:\data\SAFE\x", r"\\?\C:\data\safe", false),
            (
                r"\\?\UNC\server\share\dir\x",
                r"\\?\UNC\Server\Share\dir",
                true,
            ),
            (
                r"\\?\UNC\server\share\DIR\x",
                r"\\?\UNC\Server\Share\dir",
                false,
            ),
        ] {
            assert_eq!(
                within_root(Path::new(path), Path::new(root), false),
                allowed,
                "{path}"
            );
            assert!(
                within_root(Path::new(path), Path::new(root), true),
                "{path}"
            );
        }
    }

    #[test]
    fn symlinks_require_lexical_authority_and_resolved_target_in_any_allowed_root() {
        #[cfg(unix)]
        use std::os::unix::fs::{symlink as symlink_dir, symlink as symlink_file};
        #[cfg(windows)]
        use std::os::windows::fs::{symlink_dir, symlink_file};
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        let other = dir.path().join("other");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&other).unwrap();
        std::fs::write(root.join("file"), b"ok").unwrap();
        #[cfg(windows)]
        assert!(allowed_path(
            root.join("file").to_str().unwrap(),
            &[dir.path().join("ROOT")]
        )
        .is_ok());
        symlink_file(root.join("file"), root.join("inside")).unwrap();
        symlink_file(other.join("missing"), root.join("escape")).unwrap();
        symlink_dir(&root, dir.path().join("alias")).unwrap();
        assert_eq!(
            allowed_path(
                root.join("inside").to_str().unwrap(),
                std::slice::from_ref(&root)
            )
            .unwrap(),
            root.join("file").canonicalize().unwrap()
        );
        assert!(allowed_path(
            root.join("escape").to_str().unwrap(),
            std::slice::from_ref(&root)
        )
        .is_err());
        assert_eq!(
            allowed_path(
                root.join("escape").to_str().unwrap(),
                &[root.clone(), other.clone()]
            )
            .unwrap(),
            other.canonicalize().unwrap().join("missing")
        );
        assert!(allowed_path(
            dir.path().join("alias/file").to_str().unwrap(),
            std::slice::from_ref(&root)
        )
        .is_err());
        let looping_root = dir.path().join("loop");
        symlink_dir(&looping_root, &looping_root).unwrap();
        for roots in [
            [looping_root.clone(), root.clone()],
            [root.clone(), looping_root.clone()],
        ] {
            assert_eq!(
                allowed_path(root.join("file").to_str().unwrap(), &roots).unwrap(),
                root.join("file").canonicalize().unwrap()
            );
        }
        let error = allowed_path(
            root.join("escape").to_str().unwrap(),
            &[looping_root.clone(), root.clone()],
        )
        .unwrap_err();
        assert_eq!(
            error.local_error.unwrap().path,
            looping_root.to_string_lossy()
        );
        symlink_dir(&other, root.join("link")).unwrap();
        for (name, suffix) in [("cycle", ""), ("nested", "missing")] {
            let cycle = root.join(name);
            let target = Path::new("link").join("..").join(name).join(suffix);
            symlink_file(target, &cycle).unwrap();
            let requested = cycle.to_str().unwrap();
            let error = allowed_path(requested, std::slice::from_ref(&root)).unwrap_err();
            let local = error.local_error.unwrap();
            assert_eq!(local.path, requested);
            #[cfg(unix)]
            {
                assert_eq!(error.code, crate::ErrorCode::NotFound);
                assert_eq!(local.kind, std::io::ErrorKind::NotFound);
                assert_eq!(local.raw_os_error, Some(2));
            }
        }
        assert!(allowed_path(root.join("../other/missing").to_str().unwrap(), &[root]).is_err());
    }

    #[tokio::test]
    async fn local_errors_retain_os_code_and_requested_filename() {
        let dir = tempfile::tempdir().unwrap();
        let server = MockServer::start().await;
        for (source, kind, code) in [
            (
                dir.path().join("missing"),
                std::io::ErrorKind::NotFound,
                crate::ErrorCode::NotFound,
            ),
            (
                dir.path().to_owned(),
                if cfg!(windows) {
                    std::io::ErrorKind::PermissionDenied
                } else {
                    std::io::ErrorKind::IsADirectory
                },
                if cfg!(windows) {
                    crate::ErrorCode::PermissionDenied
                } else {
                    crate::ErrorCode::InvalidArgument
                },
            ),
        ] {
            let batch = instruction(
                "PUT",
                &format!("{}/object?secret=hidden", server.uri()),
                &source,
            );
            let state = transfer_state(1);
            let error = state
                .complete(async { Ok(batch.clone()) }, &options(dir.path()), None)
                .await
                .unwrap_err();
            assert_eq!(error.code, code);
            assert_eq!(error.error_code.as_deref(), Some("VOLUME_LOCAL_FILE_ERROR"));
            assert_eq!(error.sql_state.as_deref(), Some("HY000"));
            assert!(!error.retryable);
            let local = error.local_error.as_ref().unwrap();
            assert_eq!(local.kind, kind);
            assert!(local.raw_os_error.is_some());
            assert_eq!(local.path, source.to_str().unwrap());
            assert!(!error.message.contains("hidden"));
            assert_eq!(
                state
                    .complete(async { Ok(batch.clone()) }, &options(dir.path()), None)
                    .await
                    .unwrap_err()
                    .local_error
                    .as_ref()
                    .unwrap()
                    .raw_os_error,
                local.raw_os_error
            );
        }
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn storage_network_errors_preserve_causes_without_signed_urls() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let url = format!("http://{address}/object?secret=hidden");
        let error = Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .get(&url)
            .send()
            .await
            .unwrap_err();
        let mapped = storage_error(error);
        assert_eq!(mapped.code, crate::ErrorCode::NetworkError);
        assert_eq!(
            mapped.error_code.as_deref(),
            Some("VOLUME_STORAGE_NETWORK_ERROR")
        );
        assert!(mapped.retryable);
        assert!(
            mapped.message.to_ascii_lowercase().contains("refused"),
            "{mapped}"
        );
        assert!(!mapped.message.contains(&address.to_string()), "{mapped}");
        assert!(!mapped.message.contains("hidden"), "{mapped}");
    }

    #[tokio::test]
    async fn redirected_stream_put_replays_full_bytes() {
        let server = MockServer::start().await;
        let payload = vec![b'x'; 512 * 1024 + 1];
        Mock::given(method("PUT"))
            .and(path("/original"))
            .and(body_bytes(payload.clone()))
            .respond_with(ResponseTemplate::new(307).insert_header("location", "/target"))
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/target"))
            .and(body_bytes(payload.clone()))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        assert_eq!(
            transfer_state(0)
                .complete(
                    async {
                        Ok(instruction(
                            "PUT",
                            &format!("{}/original", server.uri()),
                            Path::new("__input_stream__"),
                        ))
                    },
                    &VolumeOptions::default(),
                    Some(payload),
                )
                .await
                .unwrap(),
            VolumeCompletion::Completed
        );
        assert!(server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| !request.headers.contains_key("referer")));
    }

    #[tokio::test]
    async fn get_decodes_gzip_content_encoding() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-encoding", "gzip")
                    .set_body_bytes(vec![
                        31, 139, 8, 0, 0, 0, 0, 0, 2, 3, 75, 73, 77, 206, 79, 73, 77, 97, 72, 170,
                        44, 73, 45, 6, 0, 196, 224, 211, 9, 13, 0, 0, 0,
                    ]),
            )
            .expect(1)
            .mount(&server)
            .await;
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("download");
        transfer_state(0)
            .complete(
                async { Ok(instruction("GET", &server.uri(), &dest)) },
                &options(dir.path()),
                None,
            )
            .await
            .unwrap();
        assert_eq!(std::fs::read(dest).unwrap(), b"decoded\x00bytes");
    }
}
