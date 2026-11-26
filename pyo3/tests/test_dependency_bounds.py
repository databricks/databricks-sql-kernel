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

"""Published dependency bounds of the wheel.

The pyarrow range is the only dependency metadata end-users resolve
against, so these tests read the installed distribution's
``Requires-Dist`` (what ``maturin develop`` / ``pip`` see) rather than
``pyproject.toml``.
"""

from __future__ import annotations

import sys
from importlib.metadata import requires

import pyarrow
import pytest
from packaging.requirements import Requirement
from packaging.version import Version

# First pyarrow release without CVE-2026-92287 (apache/arrow#51349).
PYARROW_CVE_2026_92287_FIXED = Version("26.0.0")


def pyarrow_requirement() -> Requirement:
    """The pyarrow requirement whose environment marker matches this interpreter."""
    matching = [
        req
        for req in (Requirement(r) for r in requires("databricks-sql-kernel") or [])
        if req.name == "pyarrow" and (req.marker is None or req.marker.evaluate())
    ]
    assert len(matching) == 1, (
        f"expected one pyarrow requirement for this interpreter, got {matching}"
    )
    return matching[0]


def test_installed_pyarrow_satisfies_declared_range():
    req = pyarrow_requirement()
    assert req.specifier.contains(pyarrow.__version__, prereleases=True), (
        f"installed pyarrow {pyarrow.__version__} is outside the declared range {req.specifier}"
    )


@pytest.mark.skipif(sys.version_info < (3, 10), reason="pyarrow 26 ships no Python 3.9 wheels")
def test_declared_range_admits_pyarrow_clear_of_cve_2026_92287():
    req = pyarrow_requirement()
    assert req.specifier.contains(PYARROW_CVE_2026_92287_FIXED), (
        f"declared range {req.specifier} excludes pyarrow {PYARROW_CVE_2026_92287_FIXED}; "
        "consumers could not install a pyarrow clear of CVE-2026-92287"
    )
