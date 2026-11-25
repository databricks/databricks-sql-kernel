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

"""Connection `port` kwarg: argument validation.

Every case here is rejected before any network I/O, so no workspace is
needed. A valid port is folded into the host by the kernel and exercised
by the live/e2e suites.
"""

from __future__ import annotations

import pytest

import databricks_sql_kernel as k


def _open(port, host="x.databricks.com"):
    return k.Session(
        host=host,
        http_path="/sql/1.0/warehouses/abc",
        access_token="dapi-test",
        port=port,
    )


@pytest.mark.parametrize("bad", [-1, 65536, 10**12, True, False, 8443.0, "8443"])
def test_port_out_of_range_or_non_int_raises_value_error(bad):
    with pytest.raises(ValueError, match="port must be an int between 0 and 65535"):
        _open(bad)


def test_port_with_credentials_is_rejected_without_leaking_them():
    with pytest.raises(k.KernelError) as excinfo:
        _open(8443, host="https://user:s3cret@x.databricks.com")
    assert excinfo.value.code == "InvalidArgument", excinfo.value.code
    assert "s3cret" not in str(excinfo.value)
