#!/usr/bin/env bash

run_with_redacted_url_queries() {
  local previous_shell_options="$-"
  local command_status

  set +e
  "$@" 2>&1 \
    | sed -E 's#([Hh][Tt][Tt][Pp][Ss]?://[^?[:space:]]+)\?[^[:space:]]+#\1?[REDACTED]#g'
  command_status=${PIPESTATUS[0]}
  if [[ "$previous_shell_options" == *e* ]]; then
    set -e
  fi

  return "$command_status"
}
