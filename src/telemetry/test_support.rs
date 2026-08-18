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

use super::exporter::{TelemetryExportResult, TelemetryExporter};
use super::models::TelemetryRequest;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

#[derive(Debug, Default, Clone)]
pub(crate) struct CapturingTelemetryExporter {
    requests: Arc<Mutex<Vec<TelemetryRequest>>>,
}

impl CapturingTelemetryExporter {
    pub(crate) fn requests(&self) -> Vec<TelemetryRequest> {
        self.requests
            .lock()
            .expect("poisoned telemetry capture")
            .clone()
    }
}

impl TelemetryExporter for CapturingTelemetryExporter {
    fn export<'a>(
        &'a self,
        request: TelemetryRequest,
    ) -> Pin<Box<dyn Future<Output = TelemetryExportResult> + Send + 'a>> {
        Box::pin(async move {
            self.requests
                .lock()
                .expect("poisoned telemetry capture")
                .push(request);
            Ok(())
        })
    }
}
