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

"""Rows as Python values: `fetch_all_rows` returns what pyarrow's
`to_pylist()` returns, and `SchemaKey` compares schemas."""

from __future__ import annotations

import base64
import contextlib
import datetime
import json
import random
import threading
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pyarrow as pa
import pytest

import databricks_sql_kernel as k

from conftest import live

_UTC = datetime.timezone.utc


def _converted_columns():
    return {
        "i8": pa.array([1, None, -128, 127], pa.int8()),
        "i16": pa.array([1, None, -(2**15), 2**15 - 1], pa.int16()),
        "i32": pa.array([1, None, -(2**31), 2**31 - 1], pa.int32()),
        "i64": pa.array([1, None, -(2**63), 2**63 - 1], pa.int64()),
        "u8": pa.array([1, None, 0, 255], pa.uint8()),
        "u16": pa.array([1, None, 0, 2**16 - 1], pa.uint16()),
        "u32": pa.array([1, None, 0, 2**32 - 1], pa.uint32()),
        "u64": pa.array([1, None, 0, 2**64 - 1], pa.uint64()),
        "bool": pa.array([True, None, False, True]),
        "f32": pa.array([0.1, None, float("inf"), -0.0], pa.float32()),
        "f64": pa.array([0.1, None, float("-inf"), -0.0], pa.float64()),
        "str": pa.array(["a", None, "", "é\U0001f600"]),
        "large_str": pa.array(["a", None, "", "z"], pa.large_string()),
        "bin": pa.array([b"a", None, b"", b"\x00\xff"]),
        "large_bin": pa.array([b"a", None, b"", b"\x00"], pa.large_binary()),
        "fixed_bin": pa.array([b"ab", None, b"cd", b"\x00\x00"], pa.binary(2)),
        "dec": pa.array(
            [Decimal("123141078.2283"), None, Decimal("-0.0001"), Decimal("0.0000")],
            pa.decimal128(38, 4),
        ),
        "dec_max": pa.array(
            [Decimal("9" * 38), None, Decimal("-" + "9" * 38), Decimal(0)],
            pa.decimal128(38, 0),
        ),
        "dec_tiny": pa.array(
            [Decimal("1E-20"), None, Decimal("-5E-20"), Decimal("0E-20")],
            pa.decimal128(38, 20),
        ),
        "dec_neg_scale": pa.array(
            [Decimal("1.23E+4"), None, Decimal("-1E+2"), Decimal("0E+2")],
            pa.decimal128(5, -2),
        ),
        "dec256": pa.array(
            [Decimal("1.5"), None, Decimal("-2.5"), Decimal("1" + "0" * 70 + ".5")],
            pa.decimal256(76, 1),
        ),
        "date": pa.array(
            [
                datetime.date(2024, 2, 29),
                None,
                datetime.date(1, 1, 1),
                datetime.date(9999, 12, 31),
            ]
        ),
        "ts": pa.array(
            [
                datetime.datetime(2024, 1, 1, 1, 2, 3, 4),
                None,
                datetime.datetime(1, 1, 1),
                datetime.datetime(9999, 12, 31, 23, 59, 59, 999999),
            ],
            pa.timestamp("us"),
        ),
        "ts_before_epoch": pa.array([-1, None, -86_400_000_001, -999_999], pa.timestamp("us")),
        "ts_s": pa.array([0, None, -1, 253_402_300_799], pa.timestamp("s")),
        "ts_ms": pa.array([0, None, -1, 1_710_064_800_123], pa.timestamp("ms")),
        "ts_utc": pa.array(
            [
                datetime.datetime(2024, 1, 1, tzinfo=_UTC),
                None,
                datetime.datetime(1, 1, 1, tzinfo=_UTC),
                datetime.datetime(2024, 3, 10, 10, tzinfo=_UTC),
            ],
            pa.timestamp("us", tz="Etc/UTC"),
        ),
        # The instants DST starts and ends (the latter is ambiguous locally).
        "ts_la": pa.array(
            [0, None, 1_710_064_800_000_000, 1_699_174_800_000_000],
            pa.timestamp("us", tz="America/Los_Angeles"),
        ),
        "ts_offset": pa.array([0, None, 1, 2], pa.timestamp("ms", tz="+05:30")),
        "null": pa.nulls(4),
    }


def _assert_same(got, expected):
    assert len(got) == len(expected)
    for row, expected_row in zip(got, expected):
        assert type(row) is tuple and len(row) == len(expected_row)
        for value, expected_value in zip(row, expected_row):
            assert type(value) is type(expected_value)
            assert repr(value) == repr(expected_value)
            if isinstance(value, datetime.datetime):
                assert type(value.tzinfo) is type(expected_value.tzinfo)
                assert value.tzinfo == expected_value.tzinfo
                assert value.fold == expected_value.fold


def _pylist_rows(batches):
    table = pa.Table.from_batches(batches)
    return list(zip(*(column.to_pylist() for column in table.columns))) or []


@pytest.mark.parametrize("name", sorted(_converted_columns()))
def test_values_match_to_pylist(name):
    column = _converted_columns()[name]
    batch = pa.record_batch([column, pa.array(range(len(column)))], names=[name, "k"])
    for batches in ([batch], [batch.slice(0, 1), batch.slice(1)], [batch.slice(0, 0)]):
        _assert_same(k._rows_from_arrow(batches), _pylist_rows(batches))


def test_random_values_match_to_pylist():
    rng = random.Random(7)
    n = 500
    schema = pa.schema(
        [
            ("i", pa.int64()),
            ("f", pa.float64()),
            ("d", pa.decimal128(18, 6)),
            ("dt", pa.date32()),
            ("ts", pa.timestamp("us")),
            ("tz", pa.timestamp("us", tz="Europe/Berlin")),
            ("s", pa.string()),
        ]
    )

    def maybe(v):
        return None if rng.random() < 0.1 else v

    arrays = [
        pa.array([maybe(rng.randint(-(2**63), 2**63 - 1)) for _ in range(n)], pa.int64()),
        pa.array([maybe(rng.uniform(-1e300, 1e300)) for _ in range(n)], pa.float64()),
        pa.array(
            [maybe(Decimal(rng.randint(-(10**18) + 1, 10**18 - 1)).scaleb(-6)) for _ in range(n)],
            pa.decimal128(18, 6),
        ),
        pa.array([maybe(rng.randint(-719_162, 2_932_896)) for _ in range(n)], pa.date32()),
        pa.array(
            [maybe(rng.randint(-62_135_596_800_000_000, 253_402_300_799_999_999)) for _ in range(n)],
            pa.timestamp("us"),
        ),
        pa.array(
            [maybe(rng.randint(-2_000_000_000_000_000, 4_000_000_000_000_000)) for _ in range(n)],
            pa.timestamp("us", tz="Europe/Berlin"),
        ),
        pa.array([maybe("".join(chr(rng.randint(32, 0x2FFF)) for _ in range(rng.randint(0, 8)))) for _ in range(n)]),
    ]
    batch = pa.record_batch(arrays, schema=schema)
    _assert_same(k._rows_from_arrow([batch]), _pylist_rows([batch]))


@pytest.mark.parametrize(
    "column",
    [
        pa.array([1, None], pa.timestamp("ns")),
        pa.array([0], pa.date64()),
        pa.array([0], pa.time64("us")),
        pa.array([0], pa.duration("us")),
        pa.array([[1]], pa.list_(pa.int64())),
        pa.array([{"x": 1}], pa.struct([("x", pa.int64())])),
        pa.array([[("a", 1)]], pa.map_(pa.string(), pa.int64())),
        pa.array(["a"]).dictionary_encode(),
        pa.array([pa.MonthDayNano([1, 2, 3])], pa.month_day_nano_interval()),
        # Values Python's date / datetime can't hold, where to_pylist() raises.
        pa.array([2_932_897], pa.date32()),
        pa.array([-719_163], pa.date32()),
        pa.array([253_402_300_800_000_000], pa.timestamp("us")),
        pa.array([2**62], pa.timestamp("s")),
        # Converting to the zone leaves datetime's range.
        pa.array([-62_135_596_800_000_000], pa.timestamp("us", tz="America/Los_Angeles")),
        pa.array([0], pa.timestamp("us", tz="Not/AZone")),
        pa.array([float("nan")]),
        pa.array([float("nan")], pa.float32()),
    ]
    # pyarrow < 16 (Python 3.9) has no view types.
    + ([pa.array(["a"], pa.string_view())] if hasattr(pa, "string_view") else []),
    ids=str,
)
def test_unconverted_types_and_values_decline(column):
    batch = pa.record_batch([pa.array(range(len(column))), column], names=["k", "c"])
    assert k._rows_from_arrow([batch]) is None


def test_nan_is_kept_when_allowed():
    batch = pa.record_batch([pa.array([float("nan"), 1.0, None])], names=["f"])
    rows = k._rows_from_arrow([batch], nan_ok=True)
    assert [repr(r[0]) for r in rows] == ["nan", "1.0", "None"]


def test_schema_keys_are_equal_for_equal_schemas():
    def schema(name="n", type_=pa.int32(), nullable=True, metadata=None):
        return pa.schema([pa.field(name, type_, nullable, metadata=metadata)])

    meta = {"databricks.type_name": "INT"}
    a, b = k.SchemaKey(schema(metadata=meta)), k.SchemaKey(schema(metadata=meta))
    assert a == b and hash(a) == hash(b) and len({a, b}) == 1
    for other in (
        schema(),
        schema(name="m", metadata=meta),
        schema(type_=pa.int64(), metadata=meta),
        schema(nullable=False, metadata=meta),
        schema(metadata={"databricks.type_name": "LONG"}),
    ):
        assert a != k.SchemaKey(other)
    assert a != object() and a != schema(metadata=meta)
    assert a.__eq__(object()) is NotImplemented


@contextlib.contextmanager
def _local_session(chunks):
    """A session on a local server whose every statement returns `chunks`
    inline: one list of `id` values per batch, one list of batches per chunk.
    A `None` chunk is answered with 404."""
    schema = pa.schema([("id", pa.int64())])

    def chunk(index):
        buffer = pa.BufferOutputStream()
        with pa.ipc.new_stream(buffer, schema) as writer:
            for ids in chunks[index] or []:
                writer.write_batch(pa.record_batch([pa.array(ids)], schema=schema))
        body = {
            "chunk_index": index,
            "row_offset": sum(len(ids) for c in chunks[:index] for ids in c or []),
            "row_count": sum(len(ids) for ids in chunks[index] or []),
            "attachment": base64.b64encode(buffer.getvalue()).decode(),
        }
        if index + 1 < len(chunks):
            body["next_chunk_index"] = index + 1
        return body

    response = {
        "statement_id": "test-statement",
        "status": {"state": "SUCCEEDED"},
        "manifest": {
            "format": "ARROW_STREAM",
            "schema": {
                "column_count": 1,
                "columns": [{"name": "id", "type_name": "BIGINT", "type_text": "BIGINT", "position": 0}],
            },
            "total_chunk_count": len(chunks),
            "total_row_count": sum(len(ids) for c in chunks for ids in c or []),
        },
        "result": chunk(0),
    }

    class Handler(BaseHTTPRequestHandler):
        def respond(self, body, realtime=False, status=200):
            self.rfile.read(int(self.headers.get("Content-Length", 0)))
            payload = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Length", str(len(payload)))
            if realtime:
                self.send_header("x-databricks-rt-session", "true")
            self.end_headers()
            self.wfile.write(payload)

        def do_POST(self):
            if self.path.endswith("/sessions"):
                self.respond({"session_id": "test-session"}, realtime=True)
            else:
                self.respond(response)

        def do_GET(self):
            if "/result/chunks/" in self.path:
                index = int(self.path.rsplit("/", 1)[1].split("?")[0])
                if chunks[index] is None:
                    self.respond({"error_code": "NOT_FOUND", "message": "gone"}, status=404)
                else:
                    self.respond(chunk(index))
            else:
                self.respond(response)

        def do_DELETE(self):
            self.respond({})

        def log_message(self, *_args):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        threading.Thread(target=server.serve_forever, daemon=True).start()
        session = k.Session(
            host=f"http://127.0.0.1:{server.server_port}",
            http_path="/sql/1.0/warehouses/test-wh",
            access_token="test-token",
            telemetry_enabled=False,
        )
        try:
            yield session
        finally:
            session.close()
            server.shutdown()


@pytest.fixture
def local_session():
    """Every statement returns one inline chunk of three 2-row batches."""
    with _local_session([[[0, 1], [2, 3], [4, 5]]]) as session:
        yield session


def test_a_failed_drain_keeps_the_batches_fetch_all_rows_read():
    with _local_session([[[0, 1], [2, 3]], None]) as session:
        for handle in _handles(session):
            assert handle.fetch_all_rows(1) is None
            with pytest.raises(k.KernelError):
                handle.fetch_all_arrow()
            first = handle.fetch_next_batch()
            assert first is not None and first.column(0).to_pylist() == [0, 1]
            handle.close()


def _handles(session):
    """An ExecutedStatement and a ResultStream for the local statement."""
    stmt = session.statement()
    stmt.set_sql("SELECT id FROM range(6)")
    yield stmt.execute()
    stmt = session.statement()
    stmt.set_sql("SELECT id FROM range(6)")
    yield stmt.submit().await_result()


def _ids(handle):
    ids = []
    while (batch := handle.fetch_next_batch()) is not None:
        ids.extend(batch.column(0).to_pylist())
    return ids


@pytest.mark.parametrize("max_rows", [6, 100])
def test_fetch_all_rows_reads_to_the_end(local_session, max_rows):
    for handle in _handles(local_session):
        assert handle.fetch_all_rows(max_rows) == [(i,) for i in range(6)]
        assert handle.fetch_all_rows(max_rows) == []
        assert handle.fetch_next_batch() is None
        assert handle.fetch_all_arrow().num_rows == 0
        handle.close()


@pytest.mark.parametrize("max_rows", [0, 1, 3, 5])
def test_declined_rows_are_returned_by_the_arrow_fetches(local_session, max_rows):
    for handle in _handles(local_session):
        assert handle.fetch_all_rows(max_rows) is None
        # Asking again doesn't lose or repeat what the first call read.
        assert handle.fetch_all_rows(max_rows) is None
        assert _ids(handle) == list(range(6))
        handle.close()
    for handle in _handles(local_session):
        assert handle.fetch_all_rows(max_rows) is None
        assert handle.fetch_all_arrow().column(0).to_pylist() == list(range(6))
        handle.close()


def test_fetch_all_rows_after_an_arrow_fetch_returns_the_rest(local_session):
    for handle in _handles(local_session):
        assert handle.fetch_next_batch().column(0).to_pylist() == [0, 1]
        assert handle.fetch_all_rows(4) == [(2,), (3,), (4,), (5,)]
        handle.close()


@live
def test_fetch_all_rows_returns_small_results_as_rows(session):
    stmt = session.statement()
    stmt.set_sql("SELECT 1 AS n, 'a' AS s, CAST(1.50 AS DECIMAL(5,2)) AS d")
    exec_ = stmt.execute()
    try:
        assert exec_.schema_key() == exec_.schema_key()
        assert exec_.fetch_all_rows(10) == [(1, "a", Decimal("1.50"))]
        # Exhausted afterwards, like the Arrow fetches.
        assert exec_.fetch_all_rows(10) == []
        assert exec_.fetch_next_batch() is None
    finally:
        exec_.close()
        stmt.close()


@live
def test_fetch_all_rows_leaves_larger_results_to_the_arrow_fetches(session):
    stmt = session.statement()
    stmt.set_sql("SELECT id FROM range(1000)")
    exec_ = stmt.execute()
    try:
        assert exec_.fetch_all_rows(10) is None
        ids = []
        while (batch := exec_.fetch_next_batch()) is not None:
            ids.extend(batch.column(0).to_pylist())
        assert ids == list(range(1000))
    finally:
        exec_.close()
        stmt.close()


@live
def test_fetch_all_rows_declines_unconverted_types(session):
    stmt = session.statement()
    stmt.set_sql("SELECT array(1, 2) AS a")
    exec_ = stmt.execute()
    try:
        assert exec_.fetch_all_rows(10) is None
        assert exec_.fetch_all_arrow().column(0).to_pylist() == [[1, 2]]
    finally:
        exec_.close()
        stmt.close()


@live
def test_result_stream_fetch_all_rows(session):
    stream = session.metadata().list_catalogs()
    try:
        expected = stream.arrow_schema()
        rows = stream.fetch_all_rows(100_000)
        assert rows is not None and all(len(r) == len(expected) for r in rows)
        assert stream.schema_key() == k.SchemaKey(expected)
    finally:
        stream.close()
