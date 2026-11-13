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

//! Volume completion authority and lifecycle through public execution handles.

use arrow_array::{RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use base64::{engine::general_purpose::STANDARD, Engine};
use databricks_sql_kernel::{
    AuthConfig, ConnectionConfig, DirectStatement, ErrorCode, ExecutedStatementHandle, Session,
    VolumeCompletion, VolumeOptions,
};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn response(server: &MockServer, marked: bool, local: &str) -> serde_json::Value {
    let names = ["operation", "presignedUrl", "headers", "localFile"];
    let schema = Arc::new(Schema::new(
        names
            .iter()
            .map(|name| Field::new(*name, DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ));
    let values = [
        "GET".to_owned(),
        format!("{}/object", server.uri()),
        "{}".to_owned(),
        local.to_owned(),
    ];
    let batch = RecordBatch::try_new(
        schema,
        values
            .iter()
            .map(|value| Arc::new(StringArray::from(vec![value.as_str()])) as _)
            .collect(),
    )
    .unwrap();
    let mut ipc = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc, &batch.schema()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    json!({
        "statement_id": "volume-stmt", "status": {"state": "CLOSED"},
        "manifest": {
            "format": "ARROW_STREAM", "is_volume_operation": marked,
            "schema": {"column_count":4,"columns": names.iter().enumerate().map(|(i,name)| json!({"name":name,"type_name":"STRING","type_text":"STRING","position":i})).collect::<Vec<_>>()},
            "total_row_count":1,"total_chunk_count":1
        },
        "result": {"chunk_index":0,"row_offset":0,"row_count":1,"attachment":STANDARD.encode(&ipc)}
    })
}

async fn session(server: &MockServer, body: serde_json::Value) -> Session {
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/warehouses/wh/sessions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"session_id":"sess","warehouse_id":"wh"})),
        )
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body.clone()))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/2.0/sql/statements/volume-stmt"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(server)
        .await;
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(200))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements/volume-stmt/cancel"))
        .respond_with(ResponseTemplate::new(200))
        .mount(server)
        .await;
    Session::builder()
        .connection(
            ConnectionConfig::from_http_path(server.uri(), "/sql/1.0/warehouses/wh").unwrap(),
        )
        .auth(AuthConfig::Pat {
            token: "workspace-token".into(),
            identity_federation_client_id: None,
        })
        .open()
        .await
        .unwrap()
}

#[tokio::test]
async fn sync_and_direct_closed_results_complete_explicitly_and_keep_raw_rows() {
    for direct in [false, true] {
        let server = MockServer::start().await;
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("download");
        let session = session(&server, response(&server, true, dest.to_str().unwrap())).await;
        Mock::given(method("GET"))
            .and(path("/object"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"data"))
            .expect(1)
            .mount(&server)
            .await;
        let mut stmt = session.statement();
        stmt.spec().sql("server decides the operation");
        let mut exec = if direct {
            match stmt.execute_direct().await.unwrap() {
                DirectStatement::Completed(exec) => exec,
                _ => panic!("expected inline completion"),
            }
        } else {
            stmt.execute().await.unwrap()
        };
        assert!(!dest.exists());
        let stream = exec.result_stream_mut();
        assert!(stream.is_volume_operation());
        let options = VolumeOptions {
            allowed_paths: vec![dir.path().to_owned()],
            ..Default::default()
        };
        assert_eq!(
            stream
                .complete_volume_operation(&options, None)
                .await
                .unwrap(),
            VolumeCompletion::Completed
        );
        assert_eq!(std::fs::read(&dest).unwrap(), b"data");
        let requests = server.received_requests().await.unwrap();
        let storage = requests
            .iter()
            .find(|request| request.url.path() == "/object")
            .unwrap();
        assert!(!storage.headers.contains_key("authorization"));
        assert!(exec.next_batch().await.unwrap().is_some());
        assert_eq!(
            exec.result_stream_mut()
                .complete_volume_operation(&options, None)
                .await
                .unwrap(),
            VolumeCompletion::Completed
        );
        exec.close().await.unwrap();
        session.close().await.unwrap();
    }
}

#[tokio::test]
async fn marker_false_and_id_attachment_cannot_initiate_transfer() {
    for marked in [false, true] {
        let server = MockServer::start().await;
        let session = session(&server, response(&server, marked, "/never-created")).await;
        Mock::given(method("GET"))
            .and(path("/object"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        let attached = session.attach_async_statement("volume-stmt").await.unwrap();
        let mut stream = attached.fetch_result().await.unwrap();
        assert_eq!(
            stream
                .complete_volume_operation(&VolumeOptions::default(), None)
                .await
                .unwrap(),
            VolumeCompletion::NotVolume
        );
        assert!(stream.next_batch().await.unwrap().is_some());
        if !marked {
            let mut stmt = session.statement();
            stmt.spec().sql("PUT does not authorize local effects");
            let mut exec = stmt.execute().await.unwrap();
            assert!(!exec.result_stream().is_volume_operation());
            assert_eq!(
                exec.result_stream_mut()
                    .complete_volume_operation(&VolumeOptions::default(), None)
                    .await
                    .unwrap(),
                VolumeCompletion::NotVolume
            );
        }
        session.close().await.unwrap();
    }
}

#[tokio::test]
async fn owner_reader_and_repeated_materialization_share_one_completion() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("download");
    let session = session(&server, response(&server, true, dest.to_str().unwrap())).await;
    Mock::given(method("GET"))
        .and(path("/object"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"data"))
        .expect(1)
        .mount(&server)
        .await;
    let mut stmt = session.statement();
    stmt.spec().sql("operation");
    let owner = stmt.submit().await.unwrap();
    let reader = owner.reader().unwrap();
    let mut first = reader.fetch_result().await.unwrap();
    let mut second = owner.fetch_result().await.unwrap();
    let options = VolumeOptions {
        allowed_paths: vec![dir.path().to_owned()],
        ..Default::default()
    };
    let (a, b) = tokio::join!(
        first.complete_volume_operation(&options, None),
        second.complete_volume_operation(&options, None)
    );
    assert_eq!(a.unwrap(), VolumeCompletion::Completed);
    assert_eq!(b.unwrap(), VolumeCompletion::Completed);
    drop(reader);
    assert_eq!(std::fs::read(&dest).unwrap(), b"data");
    session.close().await.unwrap();
}

#[tokio::test]
async fn cancellation_and_session_close_interrupt_transfer_after_sql_closed() {
    for close_session in [false, true] {
        let server = MockServer::start().await;
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("download");
        std::fs::write(&dest, b"old").unwrap();
        let mut session =
            Some(session(&server, response(&server, true, dest.to_str().unwrap())).await);
        Mock::given(method("GET"))
            .and(path("/object"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_secs(3))
                    .set_body_bytes(b"new"),
            )
            .mount(&server)
            .await;
        let mut stmt = session.as_ref().unwrap().statement();
        let canceller = stmt.canceller();
        stmt.spec().sql("operation");
        let mut exec = stmt.execute().await.unwrap();
        let options = VolumeOptions {
            allowed_paths: vec![dir.path().to_owned()],
            ..Default::default()
        };
        let complete = exec
            .result_stream_mut()
            .complete_volume_operation(&options, None);
        let cancel = async {
            loop {
                if server
                    .received_requests()
                    .await
                    .unwrap()
                    .iter()
                    .any(|req| req.url.path() == "/object")
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
            if close_session {
                session.take().unwrap().close().await.unwrap();
            } else {
                canceller.cancel().await.unwrap();
            }
        };
        let (result, _) = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(complete, cancel)
        })
        .await
        .unwrap();
        assert_eq!(result.unwrap_err().code, ErrorCode::Cancelled);
        assert_eq!(std::fs::read(&dest).unwrap(), b"old");
        if let Some(session) = session {
            session.close().await.unwrap();
        }
    }
}
