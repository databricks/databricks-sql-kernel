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

//! Explicit completion of server-authorized Volume file transfers.

use crate::kernel_error::{Error, LocalFileError, Result};
use crate::result_stream::ResultBatch;
use arrow_array::{Array, LargeStringArray, MapArray, StringArray, StringViewArray};
use reqwest::{header::HeaderMap, Client, Method};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;
use tokio::sync::Notify;
use tokio_util::io::ReaderStream;

/// Local file authorization and driver-specific storage response policy.
#[derive(Debug, Clone, Default)]
pub struct VolumeOptions {
    pub allowed_paths: Vec<PathBuf>,
    pub remove_not_found_is_success: bool,
}

/// Result of explicitly completing a server-marked Volume instruction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolumeCompletion {
    NotVolume,
    NeedsInputStream,
    Completed,
}

impl VolumeCompletion {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotVolume => "not_volume",
            Self::NeedsInputStream => "needs_input_stream",
            Self::Completed => "completed",
        }
    }
}

#[derive(Default)]
enum CommitState {
    #[default]
    Ready,
    Running,
    Finished(Box<Result<VolumeCompletion>>),
}

/// The synchronous gate makes cancellation and local publication mutually exclusive.
#[derive(Default)]
pub(crate) struct VolumeState {
    serial: tokio::sync::Mutex<()>,
    commit: Mutex<CommitState>,
    changed: Notify,
}

fn cancelled() -> Error {
    Error::cancelled().message("Volume transfer was cancelled")
}

impl VolumeState {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub(crate) fn cached(&self) -> Option<Result<VolumeCompletion>> {
        match &*self.commit.lock().unwrap_or_else(|e| e.into_inner()) {
            CommitState::Finished(result) => Some(result.as_ref().clone()),
            CommitState::Ready | CommitState::Running => None,
        }
    }

    pub(crate) fn fail(&self, error: Error) -> Error {
        let mut gate = self.commit.lock().unwrap_or_else(|e| e.into_inner());
        match &*gate {
            CommitState::Finished(result) => {
                if let Err(existing) = result.as_ref() {
                    return existing.clone();
                }
            }
            CommitState::Ready => *gate = CommitState::Finished(Box::new(Err(error.clone()))),
            CommitState::Running => {}
        }
        error
    }

    pub(crate) fn cancel(&self) {
        let mut gate = self.commit.lock().unwrap_or_else(|e| e.into_inner());
        if !matches!(*gate, CommitState::Finished(_)) {
            *gate = CommitState::Finished(Box::new(Err(cancelled())));
            self.changed.notify_waiters();
        }
    }

    async fn cancellation(&self) {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if matches!(self.cached(), Some(Err(error)) if error.code == crate::ErrorCode::Cancelled)
            {
                return;
            }
            notified.await;
        }
    }

    pub(crate) async fn complete(
        &self,
        batch: &ResultBatch,
        client: &Client,
        options: &VolumeOptions,
        input_bytes: Option<Vec<u8>>,
    ) -> Result<VolumeCompletion> {
        let _serial = self.serial.lock().await;
        {
            let mut gate = self.commit.lock().unwrap_or_else(|e| e.into_inner());
            match &*gate {
                CommitState::Finished(result) => return result.as_ref().clone(),
                // Holding serialization proves that the previous initializer was dropped.
                CommitState::Running => {
                    *gate = CommitState::Finished(Box::new(Err(cancelled())));
                    return Err(cancelled());
                }
                CommitState::Ready => {}
            }
        }
        let instruction = Instruction::decode(batch);
        let result = match instruction {
            Ok(instruction) => {
                if instruction.operation == "PUT"
                    && instruction.local_file == "__input_stream__"
                    && input_bytes.is_none()
                {
                    return Ok(VolumeCompletion::NeedsInputStream);
                }
                {
                    let mut gate = self.commit.lock().unwrap_or_else(|e| e.into_inner());
                    if let CommitState::Finished(result) = &*gate {
                        return result.as_ref().clone();
                    }
                    // Running is a cancellation tombstone if this future is abandoned.
                    *gate = CommitState::Running;
                }
                tokio::select! {
                    biased;
                    result = self.transfer(instruction, client, options, input_bytes) => result,
                    _ = self.cancellation() => Err(cancelled()),
                }
            }
            Err(error) => Err(error),
        };
        let mut gate = self.commit.lock().unwrap_or_else(|e| e.into_inner());
        if let CommitState::Finished(result) = &*gate {
            return result.as_ref().clone();
        }
        *gate = CommitState::Finished(Box::new(result.clone()));
        result
    }

    async fn transfer(
        &self,
        instruction: Instruction,
        client: &Client,
        options: &VolumeOptions,
        input_bytes: Option<Vec<u8>>,
    ) -> Result<VolumeCompletion> {
        let method = match instruction.operation.as_str() {
            "PUT" => Method::PUT,
            "GET" => Method::GET,
            "REMOVE" => Method::DELETE,
            _ => return Err(invalid("Unsupported Volume operation")),
        };
        let mut request = client
            .request(method, &instruction.url)
            .headers(instruction.headers);
        let local_path = if instruction.operation == "REMOVE" {
            None
        } else if instruction.operation == "PUT" && instruction.local_file == "__input_stream__" {
            let bytes = input_bytes.ok_or_else(|| invalid("Missing input stream"))?;
            request = request
                .header(reqwest::header::CONTENT_LENGTH, bytes.len())
                .body(bytes);
            None
        } else {
            let path = allowed_path(&instruction.local_file, &options.allowed_paths)?;
            if instruction.operation == "PUT" {
                let file = std::fs::File::open(&path)
                    .map_err(|e| local_error(e, &instruction.local_file))?;
                let size = file
                    .metadata()
                    .map_err(|e| local_error(e, &instruction.local_file))?
                    .len();
                request = request.header(reqwest::header::CONTENT_LENGTH, size).body(
                    reqwest::Body::wrap_stream(ReaderStream::new(tokio::fs::File::from_std(file))),
                );
            }
            Some(path)
        };
        let mut destination = if instruction.operation == "GET" {
            let path = local_path.as_ref().expect("GET path was checked");
            // Match normal write permissions even though publication uses replacement.
            let permissions = match std::fs::OpenOptions::new().write(true).open(path) {
                Ok(file) => Some(
                    file.metadata()
                        .map_err(|e| local_error(e, &instruction.local_file))?
                        .permissions(),
                ),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(local_error(error, &instruction.local_file)),
            };
            #[cfg(unix)]
            let mut builder = tempfile::Builder::new();
            #[cfg(not(unix))]
            let builder = tempfile::Builder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                builder.permissions(std::fs::Permissions::from_mode(0o666));
            }
            let temp = builder
                .tempfile_in(
                    path.parent()
                        .ok_or_else(|| invalid("Missing destination parent"))?,
                )
                .map_err(|e| local_error(e, &instruction.local_file))?;
            if let Some(permissions) = permissions {
                temp.as_file()
                    .set_permissions(permissions)
                    .map_err(|e| local_error(e, &instruction.local_file))?;
            }
            let writer = tokio::fs::File::from_std(
                temp.reopen()
                    .map_err(|e| local_error(e, &instruction.local_file))?,
            );
            Some((temp, writer))
        } else {
            None
        };

        // This client has workspace TLS/proxy/timeouts, but no workspace auth headers.
        // A single attempt avoids replaying an upload after an ambiguous response.
        let mut response = request.send().await.map_err(storage_error)?;
        let status = response.status();
        if !status.is_success()
            && !(instruction.operation == "REMOVE"
                && status.as_u16() == 404
                && options.remove_not_found_is_success)
        {
            return Err(Error::from_http_status(status.as_u16()).message(format!(
                "Volume storage request failed: HTTP {}",
                status.as_u16()
            )));
        }
        if let Some((temp, mut writer)) = destination.take() {
            while let Some(bytes) = response.chunk().await.map_err(storage_error)? {
                writer
                    .write_all(&bytes)
                    .await
                    .map_err(|e| local_error(e, &instruction.local_file))?;
            }
            writer
                .flush()
                .await
                .map_err(|e| local_error(e, &instruction.local_file))?;
            drop(writer);
            let mut gate = self.commit.lock().unwrap_or_else(|e| e.into_inner());
            if let CommitState::Finished(result) = &*gate {
                return result.as_ref().clone();
            }
            temp.persist(local_path.as_ref().expect("GET destination"))
                .map_err(|e| local_error(e.error, &instruction.local_file))?;
            *gate = CommitState::Finished(Box::new(Ok(VolumeCompletion::Completed)));
        }
        Ok(VolumeCompletion::Completed)
    }
}

fn invalid(message: &str) -> Error {
    Error::invalid_argument().message(message)
}

fn storage_error(error: reqwest::Error) -> Error {
    Error::network_error()
        .with_retryable(false)
        .message(format!(
            "Volume storage request failed: {}",
            error.without_url()
        ))
}

fn local_error(error: std::io::Error, path: &str) -> Error {
    let mut result = Error::io().message(format!("{error}: {path}"));
    result.local_error = Some(Box::new(LocalFileError {
        raw_os_error: error.raw_os_error(),
        kind: format!("{:?}", error.kind()),
        path: path.to_owned(),
    }));
    result
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

fn resolve_existing_parent(path: &Path, requested: &str) -> Result<PathBuf> {
    match path.canonicalize() {
        Ok(path) => Ok(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink()) {
                let target = std::fs::read_link(path).map_err(|e| local_error(e, requested))?;
                let target = if target.is_absolute() {
                    target
                } else {
                    path.parent()
                        .ok_or_else(|| invalid("Invalid local path"))?
                        .join(target)
                };
                return resolve_existing_parent(&normalize(&target)?, requested);
            }
            let parent = path.parent().ok_or_else(|| local_error(error, requested))?;
            let name = path
                .file_name()
                .ok_or_else(|| invalid("Invalid local path"))?;
            Ok(resolve_existing_parent(parent, requested)?.join(name))
        }
        Err(error) => Err(local_error(error, requested)),
    }
}

fn within_root(path: &Path, root: &Path) -> bool {
    #[cfg(windows)]
    {
        let mut path = path.components();
        root.components().all(|root| {
            path.next().is_some_and(|part| {
                part.as_os_str().to_string_lossy().to_lowercase()
                    == root.as_os_str().to_string_lossy().to_lowercase()
            })
        })
    }
    #[cfg(not(windows))]
    {
        path.starts_with(root)
    }
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
    if !roots.iter().any(|root| within_root(&path, root)) {
        return Err(invalid(
            "Local file path is outside the configured allowed roots",
        ));
    }
    let resolved = resolve_existing_parent(&path, requested)?;
    for root in roots {
        let root = resolve_existing_parent(&root, requested)?;
        if within_root(&resolved, &root) {
            return Ok(resolved);
        }
    }
    Err(invalid(
        "Resolved local file path is outside the configured allowed roots",
    ))
}

struct Instruction {
    operation: String,
    url: String,
    headers: HeaderMap,
    local_file: String,
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
    fn decode(batch: &ResultBatch) -> Result<Self> {
        let ResultBatch::Arrow(batch) = batch;
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
        let operation = string_at(column("operation")?, 0)?.to_owned();
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
        let local_file = if operation == "REMOVE" {
            String::new()
        } else {
            string_at(column("localFile")?, 0)?.to_owned()
        };
        Ok(Self {
            operation,
            url: string_at(column("presignedUrl")?, 0)?.to_owned(),
            headers: parsed_headers,
            local_file,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{MapBuilder, StringBuilder};
    use arrow_array::{ArrayRef, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::time::Duration;
    use wiremock::matchers::{body_bytes, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn instruction(operation: &str, url: &str, local: &Path) -> ResultBatch {
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
        ResultBatch::Arrow(RecordBatch::try_new(Arc::new(schema), arrays).unwrap())
    }

    fn options(root: &Path) -> VolumeOptions {
        VolumeOptions {
            allowed_paths: vec![root.to_owned()],
            ..Default::default()
        }
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
        let state = VolumeState::new();
        let client = Client::new();
        assert_eq!(
            state
                .complete(&batch, &client, &VolumeOptions::default(), None)
                .await
                .unwrap(),
            VolumeCompletion::NeedsInputStream
        );
        assert!(server.received_requests().await.unwrap().is_empty());
        assert_eq!(
            state
                .complete(&batch, &client, &VolumeOptions::default(), Some(vec![]))
                .await
                .unwrap(),
            VolumeCompletion::Completed
        );
        state.cancel();
        assert_eq!(
            state
                .complete(&batch, &client, &VolumeOptions::default(), Some(vec![1]))
                .await
                .unwrap(),
            VolumeCompletion::Completed
        );
    }

    #[tokio::test]
    async fn file_put_preserves_streamed_body_and_server_headers() {
        let server = MockServer::start().await;
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        let bytes = vec![42; 128 * 1024];
        std::fs::write(&source, &bytes).unwrap();
        Mock::given(method("PUT"))
            .and(body_bytes(bytes))
            .and(header("x-storage-token", "server-token"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        let batch = instruction("PUT", &server.uri(), &source);
        VolumeState::new()
            .complete(&batch, &Client::new(), &options(dir.path()), None)
            .await
            .unwrap();
        assert!(!server.received_requests().await.unwrap()[0]
            .headers
            .contains_key("authorization"));
    }

    #[tokio::test]
    async fn get_replaces_destination_and_caches_error_without_replay() {
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
        let batch = instruction("GET", &format!("{}/ok", server.uri()), &dest);
        let completed = VolumeState::new();
        completed
            .complete(&batch, &Client::new(), &options(dir.path()), None)
            .await
            .unwrap();
        completed.cancel();
        assert_eq!(
            completed
                .complete(&batch, &Client::new(), &options(dir.path()), None)
                .await
                .unwrap(),
            VolumeCompletion::Completed
        );
        assert_eq!(std::fs::read(&dest).unwrap(), b"new bytes");
        let state = VolumeState::new();
        let batch = instruction(
            "GET",
            &format!("{}/fail?secret=hidden", server.uri()),
            &dest,
        );
        let error = state
            .complete(&batch, &Client::new(), &options(dir.path()), None)
            .await
            .unwrap_err();
        assert_eq!(error.http_status, Some(403));
        assert!(!error.message.contains("hidden"));
        state.cancel();
        assert_eq!(
            state
                .complete(&batch, &Client::new(), &options(dir.path()), None)
                .await
                .unwrap_err()
                .http_status,
            Some(403)
        );
        assert_eq!(std::fs::read(&dest).unwrap(), b"new bytes");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn remove_has_narrow_missing_status_policy_and_no_local_path_requirement() {
        let server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .respond_with(ResponseTemplate::new(404))
            .expect(2)
            .mount(&server)
            .await;
        let batch = instruction("REMOVE", &server.uri(), Path::new(""));
        let strict = VolumeOptions::default();
        assert_eq!(
            VolumeState::new()
                .complete(&batch, &Client::new(), &strict, None)
                .await
                .unwrap_err()
                .http_status,
            Some(404)
        );
        let permissive = VolumeOptions {
            remove_not_found_is_success: true,
            ..strict
        };
        assert_eq!(
            VolumeState::new()
                .complete(&batch, &Client::new(), &permissive, None)
                .await
                .unwrap(),
            VolumeCompletion::Completed
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
        let client = Client::new();
        for cancel in [false, true] {
            let state = VolumeState::new();
            let mut future = Box::pin(state.complete(&batch, &client, &options, None));
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
                    .complete(&batch, &client, &options, None)
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
    async fn truncated_get_preserves_destination() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 2048];
            assert!(socket.read(&mut request).await.unwrap() > 0);
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 20\r\nConnection: close\r\n\r\nshort",
                )
                .await
                .unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("download");
        std::fs::write(&dest, b"old").unwrap();
        let batch = instruction("GET", &url, &dest);
        let error = VolumeState::new()
            .complete(&batch, &Client::new(), &options(dir.path()), None)
            .await
            .unwrap_err();
        assert_eq!(error.code, crate::ErrorCode::NetworkError);
        assert_eq!(std::fs::read(&dest).unwrap(), b"old");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
        peer.await.unwrap();
    }

    #[cfg(any(unix, windows))]
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
        assert!(allowed_path(root.join("../other/missing").to_str().unwrap(), &[root]).is_err());
    }

    #[tokio::test]
    async fn local_errors_retain_os_code_and_requested_filename() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("missing");
        let batch = instruction("PUT", "http://127.0.0.1:1?secret=hidden", &source);
        let error = VolumeState::new()
            .complete(&batch, &Client::new(), &options(dir.path()), None)
            .await
            .unwrap_err();
        let local = error.local_error.unwrap();
        assert_eq!(local.kind, "NotFound");
        assert!(local.raw_os_error.is_some());
        assert_eq!(local.path, source.to_str().unwrap());
        assert!(!error.message.contains("hidden"));
    }

    #[test]
    fn malformed_instruction_is_rejected() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "operation",
            DataType::Utf8,
            true,
        )]));
        let batch = ResultBatch::Arrow(
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["PUT"]))]).unwrap(),
        );
        assert_eq!(
            Instruction::decode(&batch).err().unwrap().code,
            crate::ErrorCode::InvalidArgument
        );
    }
}
