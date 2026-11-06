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

.PHONY: all build test fmt lint clean check coverage coverage-check c-abi-test adbc-ffi-test

all: build

build:
	cargo build
	cargo build --manifest-path adbc/Cargo.toml

test:
	cargo test
	cargo test --manifest-path adbc/Cargo.toml

fmt:
	cargo fmt --all
	cargo fmt --manifest-path adbc/Cargo.toml

fmt-check:
	cargo fmt --all -- --check
	cargo fmt --manifest-path adbc/Cargo.toml -- --check

lint:
	cargo clippy --all-targets -- -D warnings
	cargo clippy --manifest-path adbc/Cargo.toml --all-targets -- -D warnings

# Compile the hand-written C smoke tests against the cdylib and run them.
# Proves the C ABI in `src/ffi/` links and behaves from a real C consumer
# (the ODBC / Go path). Hermetic — no live warehouse needed.
c-abi-test:
	cargo build --lib
	@mkdir -p target/c-abi
	@for src in tests/c/*.c; do \
		name=$$(basename $$src .c); \
		echo "cc $$src"; \
		cc -I include $$src -L target/debug -ldatabricks_sql_kernel -o target/c-abi/$$name || exit 1; \
		LD_LIBRARY_PATH=target/debug DYLD_LIBRARY_PATH=target/debug ./target/c-abi/$$name || exit 1; \
	done

# Prove that the ADBC crate exports a loadable ADBC 1.1 C driver while its
# internal kernel integration remains native Rust.
adbc-ffi-test:
	cargo test --manifest-path adbc/Cargo.toml --features ffi
	cargo build --manifest-path adbc/Cargo.toml --features ffi --lib --example ffi_smoke
	@case "$$(uname -s)" in \
		Darwin*) library=libdatabricks_adbc.dylib ;; \
		MINGW*|MSYS*|CYGWIN*) library=databricks_adbc.dll ;; \
		*) library=libdatabricks_adbc.so ;; \
	esac; \
	LD_LIBRARY_PATH="$(CURDIR)/adbc/target/debug" \
	DYLD_LIBRARY_PATH="$(CURDIR)/adbc/target/debug" \
	./adbc/target/debug/examples/ffi_smoke "$(CURDIR)/adbc/target/debug/$$library"

check: fmt-check lint test c-abi-test adbc-ffi-test

clean:
	cargo clean
	cargo clean --manifest-path adbc/Cargo.toml

coverage:
	cargo llvm-cov --all-features --ignore-filename-regex "tests/" --html --open

coverage-check:
	cargo llvm-cov --all-features --ignore-filename-regex "tests/" --fail-under-lines 80
