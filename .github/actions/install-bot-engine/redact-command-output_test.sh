#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/redact-command-output.sh"

test_redacts_url_query_and_preserves_host_and_path() {
  local output

  output=$(run_with_redacted_url_queries bash -c \
    'printf "%s\n" "Downloading https://packages.example.test/artifacts/pkg.whl?X-Amz-Algorithm=DUMMY&X-Amz-Credential=DUMMY"')

  [[ "$output" == 'Downloading https://packages.example.test/artifacts/pkg.whl?[REDACTED]' ]]
}

test_preserves_command_exit_code() {
  local output
  local status

  set +e
  output=$(run_with_redacted_url_queries bash -c \
    'printf "%s\n" "Failed https://packages.example.test/pkg.tgz?X-Amz-Signature=DUMMY" >&2; exit 37')
  status=$?
  set -e

  [[ "$status" -eq 37 ]]
  [[ "$output" == 'Failed https://packages.example.test/pkg.tgz?[REDACTED]' ]]
}

test_redacts_url_query_and_preserves_host_and_path
test_preserves_command_exit_code
