# Copyright (c) 2026 Databricks, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Shared pytest fixtures + env-var gating for the PyO3 live tests.

Run with creds sourced:

    cd pyo3
    .venv/bin/maturin develop --release
    set -a && source ~/.databricks/pecotesting-creds && set +a
    .venv/bin/pytest -v

Tests that need a live warehouse are auto-skipped when the env vars
aren't set, so `pytest` works in CI / offline too — only the
no-network tests run.
"""

from __future__ import annotations

import os

import pytest

import databricks_sql_kernel as k


HOST = os.environ.get("DATABRICKS_HOST", "")
HTTP_PATH = os.environ.get("DATABRICKS_HTTP_PATH", "")
TOKEN = os.environ.get("DATABRICKS_TOKEN", "")

live = pytest.mark.skipif(
    not (HOST and HTTP_PATH and TOKEN),
    reason="DATABRICKS_HOST / DATABRICKS_HTTP_PATH / DATABRICKS_TOKEN not set",
)


@pytest.fixture(scope="module")
def session():
    """Open one session per test module. Closed at module teardown.

    Module-scoped because every session round-trip creates a SEA
    session on the warehouse; sharing across tests cuts E2E time by
    a factor that matters for human-driven runs.
    """
    if not (HOST and HTTP_PATH and TOKEN):
        pytest.skip("creds not set")
    s = k.Session(host=HOST, http_path=HTTP_PATH, access_token=TOKEN)
    yield s
    s.close()


@pytest.fixture
def fresh_session():
    """One-shot session for tests that need clean state (e.g.
    use-after-close tests where the module-scoped fixture would
    poison every later test)."""
    if not (HOST and HTTP_PATH and TOKEN):
        pytest.skip("creds not set")
    s = k.Session(host=HOST, http_path=HTTP_PATH, access_token=TOKEN)
    yield s
    try:
        s.close()
    except k.KernelError:
        # Test may have closed it already; idempotent close is fine.
        pass
