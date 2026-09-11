#!/usr/bin/env bash
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

set -euo pipefail

readonly PROFILE="release-with-debuginfo"
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
readonly TARGET_ROOT="${CARGO_TARGET_DIR:-${REPO_ROOT}/target}"
readonly PROFILE_DIR="${TARGET_ROOT}/${PROFILE}"
readonly CONSUMER_SOURCE="${REPO_ROOT}/tests/symbols/staticlib_consumer.c"
readonly ARCHIVE="${PROFILE_DIR}/libdatabricks_sql_kernel.a"
readonly CONSUMER="${PROFILE_DIR}/kernel-symbol-smoke"

for command in cargo cc; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "FAIL: required command not found: ${command}" >&2
    exit 1
  fi
done

mkdir -p "${PROFILE_DIR}"
build_log="$(mktemp)"
trap 'rm -f "${build_log}"' EXIT

# Cargo prints the platform libraries a C/C++ final linker must add when it
# consumes this staticlib. Capture that list while building the exact archive
# under test.
CARGO_TERM_COLOR=never cargo rustc --locked --profile "${PROFILE}" --lib -- \
  --print=native-static-libs 2>&1 | tee "${build_log}"

if [[ ! -f "${ARCHIVE}" ]]; then
  echo "FAIL: Cargo did not produce ${ARCHIVE}" >&2
  exit 1
fi

native_link_line="$(sed -n 's/^note: native-static-libs: //p' "${build_log}" | tail -n 1)"
if [[ -z "${native_link_line}" ]]; then
  echo "FAIL: rustc did not report native-static-libs" >&2
  exit 1
fi
read -r -a native_link_args <<<"${native_link_line}"

case "$(uname -s)" in
  Linux)
    for command in objcopy readelf; do
      if ! command -v "${command}" >/dev/null 2>&1; then
        echo "FAIL: required command not found: ${command}" >&2
        exit 1
      fi
    done

    debug_file="${CONSUMER}.debug"
    cc -O2 -I "${REPO_ROOT}/include" "${CONSUMER_SOURCE}" "${ARCHIVE}" \
      "${native_link_args[@]}" -Wl,--build-id=sha1 -o "${CONSUMER}"
    "${CONSUMER}"

    # This mirrors consumer packaging: retain full DWARF in the private symbol
    # artifact, strip it from the customer executable, and leave only the GNU
    # debug-link locator/checksum in the shipped file.
    objcopy --only-keep-debug "${CONSUMER}" "${debug_file}"
    objcopy --strip-debug "${CONSUMER}"
    objcopy --add-gnu-debuglink="${debug_file}" "${CONSUMER}"

    if readelf --sections "${CONSUMER}" | \
      grep -E '\.debug_(abbrev|info|line|str)' >/dev/null; then
      echo "FAIL: customer executable still contains DWARF sections" >&2
      exit 1
    fi
    if ! readelf --sections "${CONSUMER}" | grep -F '.gnu_debuglink' >/dev/null; then
      echo "FAIL: customer executable has no .gnu_debuglink" >&2
      exit 1
    fi

    dwarf_dump="${PROFILE_DIR}/kernel-symbol-smoke.dwarf.txt"
    line_dump="${PROFILE_DIR}/kernel-symbol-smoke.lines.txt"
    readelf --debug-dump=info "${debug_file}" >"${dwarf_dump}"
    readelf --debug-dump=decodedline "${debug_file}" >"${line_dump}"
    if ! grep -Fq 'kernel_get_last_error' "${dwarf_dump}"; then
      echo "FAIL: split DWARF does not describe kernel_get_last_error" >&2
      exit 1
    fi
    if ! grep -Fq 'src/c_abi/error.rs' "${line_dump}"; then
      echo "FAIL: split DWARF does not name src/c_abi/error.rs" >&2
      exit 1
    fi

    binary_notes="$(readelf --notes "${CONSUMER}")"
    debug_notes="$(readelf --notes "${debug_file}")"
    binary_id="$(sed -n 's/.*Build ID: //p' <<<"${binary_notes}")"
    debug_id="$(sed -n 's/.*Build ID: //p' <<<"${debug_notes}")"
    if [[ -z "${binary_id}" || "${binary_id}" != "${debug_id}" ]]; then
      echo "FAIL: executable and split DWARF build IDs do not match" >&2
      exit 1
    fi

    echo "ok: Linux split DWARF contains kernel Rust source/function info"
    echo "ok: executable and .debug share build ID ${binary_id}"
    ;;
  Darwin)
    for command in dsymutil otool strip xcrun; do
      if ! command -v "${command}" >/dev/null 2>&1; then
        echo "FAIL: required command not found: ${command}" >&2
        exit 1
      fi
    done

    dsym="${CONSUMER}.dSYM"
    cc -O2 -I "${REPO_ROOT}/include" "${CONSUMER_SOURCE}" "${ARCHIVE}" \
      "${native_link_args[@]}" -o "${CONSUMER}"
    "${CONSUMER}"
    dsymutil "${CONSUMER}" -o "${dsym}"
    strip -S "${CONSUMER}"

    if otool -l "${CONSUMER}" | grep -F '__DWARF' >/dev/null; then
      echo "FAIL: customer executable still contains a __DWARF segment" >&2
      exit 1
    fi

    dwarf_dump="${PROFILE_DIR}/kernel-symbol-smoke.dwarf.txt"
    dwarf_file="${dsym}/Contents/Resources/DWARF/$(basename "${CONSUMER}")"
    if [[ ! -f "${dwarf_file}" ]]; then
      echo "FAIL: dsymutil did not produce ${dwarf_file}" >&2
      exit 1
    fi
    xcrun dwarfdump --name kernel_get_last_error "${dsym}" >"${dwarf_dump}"
    if ! grep -Fq 'kernel_get_last_error' "${dwarf_dump}"; then
      echo "FAIL: dSYM does not describe kernel_get_last_error" >&2
      exit 1
    fi
    if ! grep -Eq 'DW_AT_decl_file.*error\.rs' "${dwarf_dump}"; then
      echo "FAIL: kernel_get_last_error has no error.rs source record in the dSYM" >&2
      exit 1
    fi

    binary_uuid_dump="$(xcrun dwarfdump --uuid "${CONSUMER}")"
    dsym_uuid_dump="$(xcrun dwarfdump --uuid "${dsym}")"
    binary_uuid="$(awk 'NR == 1 { print $2 }' <<<"${binary_uuid_dump}")"
    dsym_uuid="$(awk 'NR == 1 { print $2 }' <<<"${dsym_uuid_dump}")"
    if [[ -z "${binary_uuid}" || "${binary_uuid}" != "${dsym_uuid}" ]]; then
      echo "FAIL: executable and dSYM UUIDs do not match" >&2
      exit 1
    fi

    echo "ok: macOS dSYM contains kernel Rust source/function info"
    echo "ok: executable and dSYM share UUID ${binary_uuid}"
    ;;
  *)
    echo "FAIL: unsupported platform for this check: $(uname -s)" >&2
    exit 1
    ;;
esac
