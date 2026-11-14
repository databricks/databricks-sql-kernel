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

"""Kernel records reach Python ``logging`` at the levels set there.

No network: ``Session`` without credentials raises before any I/O, after a
DEBUG breadcrumb on ``databricks.sql.kernel.pyo3``.
"""

from __future__ import annotations

import logging

import pytest

import databricks_sql_kernel as k

KERNEL_LOGGER = "databricks.sql.kernel"
REJECTED = "Session.__new__ rejected"


def _reject_session() -> None:
    with pytest.raises(ValueError):
        k.Session(host="example.invalid", http_path="/sql/1.0/warehouses/x")


def _breadcrumbs(caplog) -> list:
    return [
        r
        for r in caplog.records
        if r.name.startswith(KERNEL_LOGGER) and REJECTED in r.getMessage()
    ]


@pytest.fixture(autouse=True)
def _restore_levels():
    yield
    k.reset_logging()


def test_debug_set_after_import_reaches_python(caplog):
    # The bridge was installed at import, before this level was set.
    with caplog.at_level(logging.DEBUG, logger=KERNEL_LOGGER):
        _reject_session()
        assert k._log_max_level() == "DEBUG"
    (record,) = _breadcrumbs(caplog)
    assert record.name == "databricks.sql.kernel.pyo3"
    assert record.levelno == logging.DEBUG


def test_records_below_the_level_are_dropped_before_python(caplog):
    with caplog.at_level(logging.WARNING, logger=KERNEL_LOGGER):
        k.reset_logging()
        # Below WARNING nothing is forwarded, so DEBUG records are dropped
        # by `log`'s own level check instead of reaching pyo3_log.
        assert k._log_max_level() == "WARN"
        _reject_session()
        assert k._log_max_level() == "WARN"
    assert not _breadcrumbs(caplog)


def test_new_session_picks_up_a_level_changed_after_caching(caplog):
    # Another logger at DEBUG keeps the max level at Debug, so the
    # breadcrumb reaches pyo3_log and is cached at the kernel logger's
    # WARNING level without being forwarded.
    other = logging.getLogger("test_logging.unrelated")
    other.setLevel(logging.DEBUG)
    try:
        with caplog.at_level(logging.WARNING, logger=KERNEL_LOGGER):
            k.reset_logging()
            _reject_session()
        assert not _breadcrumbs(caplog)
        # Raising the kernel logger leaves the max level unchanged; the new
        # Session still has to drop the stale cache entry.
        with caplog.at_level(logging.DEBUG, logger=KERNEL_LOGGER):
            _reject_session()
        assert len(_breadcrumbs(caplog)) == 1
    finally:
        other.setLevel(logging.NOTSET)


def test_reset_logging_is_safe_to_call_repeatedly():
    k.reset_logging()
    k.reset_logging()
