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

"""Metadata surface: list_*, type_info, table_types, validation,
ResultStream lifecycle."""

from __future__ import annotations

import pyarrow as pa
import pytest

import databricks_sql_kernel as k

from conftest import live


@live
def test_list_catalogs_returns_non_empty(session):
    rs = session.metadata().list_catalogs()
    try:
        assert isinstance(rs.arrow_schema(), pa.Schema)
        table = rs.fetch_all_arrow()
        assert isinstance(table, pa.Table)
        assert table.num_rows > 0
    finally:
        rs.close()


@live
def test_list_schemas_in_main_catalog(session):
    rs = session.metadata().list_schemas(catalog="main")
    try:
        table = rs.fetch_all_arrow()
        assert table.num_rows > 0
    finally:
        rs.close()


@live
def test_list_tables_in_information_schema(session):
    rs = session.metadata().list_tables(
        catalog="system", schema_pattern="information_schema"
    )
    try:
        table = rs.fetch_all_arrow()
        assert table.num_rows > 0
        # Server-shape sanity: the columns should match the kernel's
        # documented `SHOW TABLES` server-shape.
        col_names = [f.name for f in table.schema]
        for required in ("tableName", "tableType"):
            assert required in col_names, f"missing column {required!r}: {col_names}"
    finally:
        rs.close()


@live
def test_list_columns_in_tables(session):
    rs = session.metadata().list_columns(
        catalog="system",
        schema_pattern="information_schema",
        table_pattern="tables",
    )
    try:
        table = rs.fetch_all_arrow()
        assert table.num_rows > 0
    finally:
        rs.close()


@live
def test_type_info_is_static(session):
    rs = session.metadata().type_info()
    try:
        table = rs.fetch_all_arrow()
        assert table.num_rows > 0
    finally:
        rs.close()


@live
def test_table_types_is_static(session):
    rs = session.metadata().table_types()
    try:
        table = rs.fetch_all_arrow()
        assert table.num_rows > 0
    finally:
        rs.close()


@live
def test_empty_identifier_rejected(session):
    try:
        session.metadata().list_schemas(catalog="")
    except k.KernelError as e:
        assert e.code == "InvalidArgument", e.code
        assert "identifier" in e.message.lower(), e.message
    else:
        raise AssertionError("expected KernelError for empty catalog")


@live
def test_list_columns_requires_catalog_kwarg(session):
    # `catalog` is a kwarg-only required parameter (no default).
    with pytest.raises(TypeError):
        session.metadata().list_columns(schema_pattern="x")  # type: ignore[call-arg]


@live
def test_result_stream_idempotent_close(session):
    rs = session.metadata().list_catalogs()
    rs.close()
    rs.close()  # idempotent
    try:
        rs.fetch_next_batch()
    except k.KernelError as e:
        assert "closed" in str(e), str(e)
        assert e.code == "InvalidStatementHandle", e.code
    else:
        raise AssertionError("expected KernelError on use-after-close ResultStream")
