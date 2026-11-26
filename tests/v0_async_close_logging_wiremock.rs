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

use databricks_sql_kernel::{AuthConfig, ExecutedAsyncStatement, LoggingConfig, Session};
use serde_json::json;
use std::time::Duration;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn background_close_failures_preserve_origin_and_session_lifecycle() {
    let directory = tempfile::tempdir().unwrap();
    let log_path = directory.path().join("close.log");
    for (index, (drained, close_session, realtime)) in [
        (true, false, false),
        (true, true, false),
        (true, true, true),
        (false, false, false),
    ]
    .into_iter()
    .enumerate()
    {
        let offset = std::fs::read(&log_path).map_or(0, |contents| contents.len());
        let server = MockServer::start().await;
        let statement_id = format!("stmt-close-error-{index}");
        let session_path = if realtime {
            "/api/2.0/sql/warehouses/test_wh/sessions/sess-test"
        } else {
            "/api/2.0/sql/sessions/sess-test"
        };
        let statement_path = if realtime {
            format!("{session_path}/statements")
        } else {
            "/api/2.0/sql/statements".into()
        };
        let mut created =
            ResponseTemplate::new(200).set_body_json(json!({"session_id": "sess-test"}));
        if realtime {
            created = created.insert_header("x-databricks-rt-session", "true");
        }
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/warehouses/test_wh/sessions"))
            .respond_with(created)
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path(session_path))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(&statement_path))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "statement_id": statement_id,
                "status": {"state": "PENDING"}
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("{statement_path}/{statement_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "statement_id": statement_id,
                "status": {"state": "SUCCEEDED"},
                "manifest": {
                    "format": "ARROW_STREAM",
                    "schema": {"column_count": 0, "columns": []},
                    "total_chunk_count": 0, "total_row_count": 0
                }
            })))
            .mount(&server)
            .await;
        let delete_path = format!("{statement_path}/{statement_id}");
        Mock::given(method("DELETE"))
            .and(path(&delete_path))
            .respond_with(
                ResponseTemplate::new(400)
                    .set_body_json(json!({"error_code": "BAD_REQUEST", "message": "rejected"}))
                    .set_delay(Duration::from_millis(400)),
            )
            .expect(1)
            .mount(&server)
            .await;
        let mut logging = LoggingConfig::default();
        logging.filter = Some("databricks::sql::kernel=debug".into());
        logging.file = Some(log_path.clone());
        let session = Session::builder()
            .http_path(server.uri(), "/sql/1.0/warehouses/test_wh")
            .auth(AuthConfig::Pat {
                token: "dapi_test_token".into(),
                identity_federation_client_id: None,
            })
            .logging(logging)
            .open()
            .await
            .unwrap();
        let mut statement = session.statement();
        statement.spec().sql("SELECT 1");
        let mut owner = statement.submit().await.unwrap();
        if drained {
            let mut stream = owner.await_result().await.unwrap();
            assert!(stream.next_batch().await.unwrap().is_none());
            tokio::time::timeout(
                Duration::from_millis(150),
                ExecutedAsyncStatement::close(&mut owner),
            )
            .await
            .expect("drained close must return before the rejected DELETE")
            .unwrap();
        }
        drop(owner);
        drop(statement);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let requests = server.received_requests().await.unwrap();
                if requests.iter().any(|request| {
                    request.method.as_str() == "DELETE" && request.url.path() == delete_path
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("statement DELETE must begin");
        let session = if close_session {
            session.close().await.unwrap();
            None
        } else {
            Some(session)
        };
        let expected_message = if close_session {
            "background close_statement after drained async close failed after session closure"
        } else if drained {
            "background close_statement after drained async close failed; no Drop retry will run"
        } else {
            "fire-and-forget close_statement on ExecutedAsyncStatement::Drop failed; server-side resource reclaimed when the session closes"
        };
        let captured = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let contents = std::fs::read_to_string(&log_path).unwrap();
                let captured = &contents[offset..];
                if captured.contains(expected_message) {
                    break captured.to_owned();
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("cleanup must report its origin at the expected level");
        let warnings = captured
            .lines()
            .filter(|line| line.contains(" WARN "))
            .count();
        assert_eq!(
            warnings,
            if close_session {
                0
            } else if drained {
                1
            } else {
                2
            },
            "{captured}"
        );
        assert!(!captured.contains(" ERROR "), "{captured}");
        if drained {
            assert!(
                !captured.contains("Drop fallback close will run"),
                "{captured}"
            );
        }
        if close_session {
            assert!(captured
                .lines()
                .any(|line| line.contains(" DEBUG ") && line.contains(expected_message)));
        }
        if let Some(session) = session {
            session.close().await.unwrap();
        }
        server.verify().await;
    }
}
