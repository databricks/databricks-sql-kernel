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

import inspect

import databricks_sql_kernel as k

from conftest import live


def test_revocation_kwargs_are_keyword_only_and_default_off():
    # Offline: checks the constructor contract. Unset (None) means off.
    params = inspect.signature(k.Session).parameters
    for name in (
        "tls_check_certificate_revocation",
        "tls_accept_undetermined_revocation",
    ):
        assert params[name].kind is inspect.Parameter.KEYWORD_ONLY, name
        assert params[name].default is None, name


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


@live
def test_session_conf_round_trips():
    """`session_conf` is passed through to SEA `session_configuration`
    verbatim. Setting `ANSI_MODE=false` proves the map reached the
    server and was honoured — `SET ANSI_MODE` reflects it back.
    `ANSI_MODE` was chosen because it's on the allow-list this
    warehouse accepts and round-trips cleanly (`TIMEZONE` is
    rejected with "Unsupported configuration" on dogfood;
    `USE_CACHED_RESULT` / `STATEMENT_TIMEOUT` are
    workspace-policy-clamped).
    """
    # Pull from the same module-level constants conftest defines so
    # offline pytest runs short-circuit via @live before this body
    # ever evaluates — no `os.environ[...]` KeyError hazard.
    from conftest import HOST, HTTP_PATH, TOKEN

    s = k.Session(
        host=HOST,
        http_path=HTTP_PATH,
        access_token=TOKEN,
        session_conf={"ANSI_MODE": "false"},
    )
    try:
        stmt = s.statement()
        stmt.set_sql("SET ANSI_MODE")
        exec_ = stmt.execute()
        try:
            table = exec_.fetch_all_arrow()
            # Server returns rows like (key='ANSI_MODE', value='false').
            row = {f.name: table.column(i).to_pylist()[0] for i, f in enumerate(table.schema)}
            assert row.get("value") == "false", (
                f"server did not honour ANSI_MODE=false; got row {row!r}"
            )
        finally:
            exec_.close()
            stmt.close()
    finally:
        s.close()
