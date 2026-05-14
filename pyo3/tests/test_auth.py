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

"""Auth surface: input validation + External-callback trampoline.

The trampoline itself (Python -> Rust -> kernel `TokenProvider`) is
covered by Rust unit tests in `pyo3/src/auth.rs`. These tests cover
the Python-visible surface: kwargs accepted / rejected, and that
`auth_type='external'` reaches the kernel (which today rejects the
variant — see the known-limitation note on the PR).
"""

from __future__ import annotations

import os

import pytest

import databricks_sql_kernel as k


HOST = os.environ.get("DATABRICKS_HOST", "")
HTTP_PATH = os.environ.get("DATABRICKS_HTTP_PATH", "")
TOKEN = os.environ.get("DATABRICKS_TOKEN", "")


# Input-validation tests don't need creds — they fail before we
# build a runtime / hit the wire.

def test_missing_auth_raises_value_error():
    with pytest.raises(ValueError, match="auth"):
        k.Session(host="x.databricks.com", http_path="/sql/1.0/warehouses/abc")


def test_external_without_callback_raises_value_error():
    with pytest.raises(ValueError, match="token_callback"):
        k.Session(
            host="x.databricks.com",
            http_path="/sql/1.0/warehouses/abc",
            auth_type="external",
        )


def test_external_with_non_callable_raises_value_error():
    with pytest.raises(ValueError, match="callable"):
        k.Session(
            host="x.databricks.com",
            http_path="/sql/1.0/warehouses/abc",
            auth_type="external",
            token_callback="not-callable",
        )


def test_pat_with_empty_token_raises_value_error():
    with pytest.raises(ValueError, match="access_token"):
        k.Session(
            host="x.databricks.com",
            http_path="/sql/1.0/warehouses/abc",
            auth_type="pat",
            access_token="",
        )


def test_unknown_auth_type_raises_value_error():
    with pytest.raises(ValueError, match="unknown auth_type"):
        k.Session(
            host="x.databricks.com",
            http_path="/sql/1.0/warehouses/abc",
            auth_type="something-else",
            access_token="x",
        )


# The end-to-end-with-callback test does need creds because it
# actually tries to open a session (which today the kernel rejects
# at the External arm of build_auth_provider). The kernel-side
# follow-up will flip this from "expected InvalidArgument" to
# "expected success."
@pytest.mark.skipif(
    not (HOST and HTTP_PATH and TOKEN),
    reason="DATABRICKS_HOST / DATABRICKS_HTTP_PATH / DATABRICKS_TOKEN not set",
)
def test_external_reaches_kernel_today():
    """The kernel currently rejects AuthConfig::External at
    build_auth_provider. This test pins that behavior so the
    kernel-side follow-up PR will flip a clear assertion when it
    enables the variant."""

    def callback():
        return TOKEN

    try:
        k.Session(
            host=HOST,
            http_path=HTTP_PATH,
            auth_type="external",
            token_callback=callback,
        )
    except k.KernelError as e:
        assert e.code == "InvalidArgument", e.code
        assert "External" in e.message, e.message
    else:
        # If this branch ever executes, the kernel-side PR has
        # landed — update this test to assert success instead.
        raise AssertionError(
            "Session(auth_type='external') succeeded — the kernel "
            "has wired AuthConfig::External. Update this test."
        )
