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

"""Session lifecycle: open, session_id, repr, idempotent close,
use-after-close."""

from __future__ import annotations

import databricks_sql_kernel as k

from conftest import live


@live
def test_session_id_non_empty(session):
    assert session.session_id, f"session_id should be populated: {session.session_id!r}"


@live
def test_session_repr_open(session):
    text = repr(session)
    assert "Session" in text
    assert "closed=False" in text
    assert session.session_id in text


@live
def test_double_close_is_idempotent(fresh_session):
    fresh_session.close()
    # Second call must not raise.
    fresh_session.close()


@live
def test_use_after_close_raises_kernel_error(fresh_session):
    fresh_session.close()
    try:
        fresh_session.statement()
    except k.KernelError as e:
        assert "closed" in str(e), str(e)
        assert e.code == "InvalidStatementHandle", e.code
    else:
        raise AssertionError("expected KernelError on use-after-close")
