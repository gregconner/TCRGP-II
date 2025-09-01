#!/usr/bin/env bash
# Usage: write_grok4_output.sh <output_path> <delimiter>
# Reads STDIN until <delimiter> and writes to <output_path> safely.
set -euo pipefail
if [[ ${#} -ne 2 ]]; then
  echo "Usage: $0 <output_path> <delimiter>" >&2
  exit 1
fi
out="$1"
delim="$2"
# Ensure parent dir exists
mkdir -p "$(dirname "$out")"
# Write until delimiter
# shellcheck disable=SC2162
{
  : > "$out"
  while IFS= read -r line; do
    if [[ "$line" == "$delim" ]]; then
      break
    fi
    printf '%s\n' "$line" >> "$out"
  done
}
# Confirm without printing content
ls -l "$out"
