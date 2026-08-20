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

"""Module-level sanity: import, version, exported classes."""

from __future__ import annotations

import databricks_sql_kernel as k


def test_version_string():
    assert isinstance(k.__version__, str)
    assert k.__version__


def test_exports_expected_classes():
    expected = {
        "Session",
        "Statement",
        "ExecutedStatement",
        "ExecutedAsyncStatement",
        "Metadata",
        "ResultStream",
        "KernelError",
    }
    actual = {n for n in dir(k) if not n.startswith("_")}
    assert expected.issubset(actual), f"missing exports: {expected - actual}"


def test_statement_exposes_row_limit():
    assert callable(k.Statement.set_row_limit)
