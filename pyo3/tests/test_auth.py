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

"""Auth surface: input validation.

PAT-only this phase; tests confirm the wrapper rejects unsupported
kwargs at construction time (before any network) rather than
letting the kernel error mid-flight.
"""

from __future__ import annotations

import pytest

import databricks_sql_kernel as k


def test_missing_auth_raises_value_error():
    with pytest.raises(ValueError, match="access_token"):
        k.Session(host="x.databricks.com", http_path="/sql/1.0/warehouses/abc")


def test_pat_with_empty_token_raises_value_error():
    with pytest.raises(ValueError, match="access_token"):
        k.Session(
            host="x.databricks.com",
            http_path="/sql/1.0/warehouses/abc",
            auth_type="pat",
            access_token="",
        )


def test_unsupported_auth_type_raises_value_error():
    with pytest.raises(ValueError, match="unsupported"):
        k.Session(
            host="x.databricks.com",
            http_path="/sql/1.0/warehouses/abc",
            auth_type="external",
            access_token="x",
        )


def test_unknown_auth_type_raises_value_error():
    with pytest.raises(ValueError, match="unsupported"):
        k.Session(
            host="x.databricks.com",
            http_path="/sql/1.0/warehouses/abc",
            auth_type="something-else",
            access_token="x",
        )


def test_identity_federation_client_id_is_accepted_kwarg():
    # The SP-wide WIF selector is a recognized keyword (not a TypeError):
    # paired with missing auth it reaches the pre-network auth rejection,
    # which proves the signature accepts it without opening a connection.
    with pytest.raises(ValueError, match="access_token"):
        k.Session(
            host="x.databricks.com",
            http_path="/sql/1.0/warehouses/abc",
            identity_federation_client_id="sp-client-abc",
        )


def test_unknown_kwarg_raises_type_error():
    # Guards the test above: a genuinely unknown kwarg is a TypeError, so
    # the ValueError in the test above really does mean the kwarg was
    # accepted by the signature.
    with pytest.raises(TypeError):
        k.Session(
            host="x.databricks.com",
            http_path="/sql/1.0/warehouses/abc",
            access_token="x",
            not_a_real_kwarg="nope",
        )
