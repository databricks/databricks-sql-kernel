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

//! Wiremock test for `kernel_session_config_set_cloudfetch_enabled`: setting
//! it to `false` must make the kernel send `can_cloud_download=false` in the
//! create-session request's `session_confs`, so SEA serves results inline
//! instead of via CloudFetch. Asserting the wire body is the substitute for a
//! live e2e — a real warehouse gates `can_cloud_download` (returns
//! `CONFIG_NOT_AVAILABLE / 42K0I`), so the toggle can't be verified against
//! staging; this proves the toggle reaches the wire.
//!
//! Same threading shape as the other C-ABI wiremock tests: the C ABI drives
//! its own runtime inside `block_on_ffi`, so the entry points run on a plain
//! `#[test]` thread with a dedicated runtime only to host the mock.

use std::ffi::CString;

use databricks_sql_kernel::c_abi::session::{
    kernel_session_close, kernel_session_config_new, kernel_session_config_set_auth_pat,
    kernel_session_config_set_cloudfetch_enabled, kernel_session_config_set_http_path,
    kernel_session_open, kernel_session_t, KernelSessionConfig,
};
use databricks_sql_kernel::c_abi::KernelStatusCode;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[test]
fn set_cloudfetch_disabled_sends_can_cloud_download_false() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(async {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(r#"{"session_id":"sess-cf","warehouse_id":"test_wh"}"#),
            )
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/sess-cf"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        server
    });
    let uri = server.uri();

    unsafe {
        let mut config: *mut KernelSessionConfig = std::ptr::null_mut();
        assert_eq!(
            kernel_session_config_new(&mut config),
            KernelStatusCode::Success
        );
        let host = CString::new(uri.as_str()).unwrap();
        let http_path = CString::new("/sql/1.0/warehouses/test_wh").unwrap();
        assert_eq!(
            kernel_session_config_set_http_path(config, host.as_ptr(), http_path.as_ptr()),
            KernelStatusCode::Success
        );
        let token = CString::new("dapi_test_token").unwrap();
        assert_eq!(
            kernel_session_config_set_auth_pat(config, token.as_ptr()),
            KernelStatusCode::Success
        );
        // Disable CloudFetch — the assertion target.
        assert_eq!(
            kernel_session_config_set_cloudfetch_enabled(config, false),
            KernelStatusCode::Success
        );

        let mut session: *mut kernel_session_t = std::ptr::null_mut();
        assert_eq!(
            kernel_session_open(config, &mut session),
            KernelStatusCode::Success
        );
        assert!(!session.is_null());
        // Tidy up (fire-and-forget close is fine here — we only inspect the
        // create-session request below).
        kernel_session_close(session);
    }

    // Inspect the create-session POST body: session_confs must carry
    // can_cloud_download=false.
    let body = rt.block_on(async {
        let requests = server.received_requests().await.unwrap_or_default();
        let create = requests
            .iter()
            .find(|r| r.method.as_str() == "POST" && r.url.path().ends_with("/sql/sessions"))
            .expect("a create-session POST must have been sent");
        serde_json::from_slice::<serde_json::Value>(&create.body).expect("create body is JSON")
    });
    let confs = &body["session_confs"];
    let val = confs
        .get("can_cloud_download")
        .or_else(|| confs.get("can_cloud_download".to_uppercase().as_str()));
    assert_eq!(
        val.and_then(|v| v.as_str()),
        Some("false"),
        "disabling CloudFetch must send can_cloud_download=false in session_confs, got confs={confs}"
    );
}
