<!--
  Copyright (c) 2026 Databricks, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

          http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Release debuginfo for statically linked consumers

ODBC, ADBC, and other native consumers can retain Rust source and function
information without changing the code they ship. The kernel provides the
opt-in `release-with-debuginfo` Cargo profile:

```bash
cargo build --locked --profile release-with-debuginfo --lib
```

The static library is written beneath `target/release-with-debuginfo/` as
`libdatabricks_sql_kernel.a` on Linux/macOS or
`databricks_sql_kernel.lib` on Windows.

The profile inherits Cargo's existing `release` profile and overrides only
`debug = 2`. Consequently its optimization level, LTO setting, codegen-unit
count, panic strategy, overflow checks, and debug assertions are identical to
an ordinary `cargo build --release`. Do not substitute a toolchain-specific
"RelWithDebInfo" preset that may also change optimization.

## Consumer workspaces

Cargo reads profiles only from the workspace root. If the kernel is built as a
package inside a larger Rust workspace, repeat this profile in that workspace's
root `Cargo.toml`:

```toml
[profile.release-with-debuginfo]
inherits = "release"
debug = 2
```

Then select it for the build that produces the kernel static library.
Corrosion-based native consumers can select the kernel profile on their crate
import:

```cmake
corrosion_import_crate(
    MANIFEST_PATH "${databricks_sql_kernel_SOURCE_DIR}/Cargo.toml"
    CRATE_TYPES staticlib
    PROFILE release-with-debuginfo
)
```

Consumers that cannot select a named profile can opt in for one release
invocation with Cargo's profile environment setting:

```bash
CARGO_PROFILE_RELEASE_DEBUG=2 cargo build --locked --release
```

The named profile is preferred because it uses a separate target directory and
cannot accidentally reuse a plain-release artifact.

## Link and publish symbols

Debug records in the kernel archive are intermediate input. The native
consumer's final linker copies records only for archive members used by the
driver. Generate the platform symbol artifact from that exact final link, then
strip/package the customer binary separately:

- **Linux:** give the final ELF a GNU build ID, run `objcopy
  --only-keep-debug` to create `<driver>.debug`, strip debug sections from the
  shipped driver, and add a `.gnu_debuglink`. Publish the `.debug` file only in
  the private symbol channel. The ELF build ID identifies the binary; the
  debuglink also records the companion file's CRC.
- **macOS:** run `dsymutil <driver> -o <driver>.dSYM` before stripping or
  deleting the kernel archive/object inputs, then strip the packaged driver.
  Publish only the `.dSYM` through the symbol channel. The Mach-O UUID printed
  by `dwarfdump --uuid` must match the dSYM UUID.
- **Windows (MSVC):** compile the kernel with the profile above and link the
  final driver with `/DEBUG:FULL /OPT:REF /OPT:ICF /INCREMENTAL:NO
  /PDB:<driver>.pdb`. The explicit optimization and incremental-link options
  retain Release linker behavior that `/DEBUG` would otherwise change.
  `/PDBALTPATH:%_PDB%` avoids embedding a build-machine path. Package the DLL
  without the PDB and publish the PDB only through the symbol channel. The PE
  CodeView GUID+Age identifies the one matching PDB.

Never rebuild a binary merely to produce its symbols: optimization and link
layout can change addresses. Archive the customer binary and its separate
symbol artifact from one build, indexed by the platform identity above.

The `Release Debuginfo` CI workflow exercises the complete static-library and
native-link path on all three platforms. It recognizes
`kernel_get_last_error` and `src/c_abi/error.rs` in the resulting `.debug`,
`.dSYM`, or linker `.pdb`, verifies the binary/symbol identity, and checks that
the customer executable does not retain the private debug payload.
