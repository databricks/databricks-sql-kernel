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

use super::models::{TelemetryFrontendLog, TelemetryRequest};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

pub(crate) type TelemetryExportResult = Result<(), TelemetryExportError>;

#[derive(Debug, thiserror::Error)]
pub(crate) enum TelemetryExportError {
    #[error("telemetry serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("telemetry export failed: {0}")]
    Export(String),
    /// The endpoint rate-limited the export (HTTP 429 or 503). The batch was
    /// not accepted and can be sent again later.
    #[error("telemetry export throttled: {0}")]
    Throttled(String),
}

pub(crate) trait TelemetryExporter: Send + Sync {
    fn export<'a>(
        &'a self,
        request: TelemetryRequest,
    ) -> Pin<Box<dyn Future<Output = TelemetryExportResult> + Send + 'a>>;
}

pub(super) async fn export_batch(
    exporter: Arc<dyn TelemetryExporter>,
    batch: &[TelemetryFrontendLog],
) -> TelemetryExportResult {
    let request = TelemetryRequest::from_logs(batch).inspect_err(|e| {
        tracing::debug!(error = %e, "failed to serialize telemetry batch");
    })?;

    exporter.export(request).await.inspect_err(|e| {
        tracing::debug!(error = %e, "failed to export telemetry batch");
    })
}
