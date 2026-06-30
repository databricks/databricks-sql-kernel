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

.PHONY: all build test fmt lint clean check coverage coverage-check c-abi-test

all: build

build:
	cargo build

test:
	cargo test

fmt:
	cargo fmt

fmt-check:
	cargo fmt -- --check

lint:
	cargo clippy -- -D warnings

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

check: fmt-check lint test c-abi-test

clean:
	cargo clean

coverage:
	cargo llvm-cov --all-features --ignore-filename-regex "tests/" --html --open

coverage-check:
	cargo llvm-cov --all-features --ignore-filename-regex "tests/" --fail-under-lines 80
