#!/usr/bin/env bash
set -euo pipefail
if [[ $# -ne 11 ]]; then
  echo 'Expected 11 arguments' >&2
  exit 2
fi
[[ "$1" == load_posting_new_file && "$2" =~ ^[0-9]{8}$ && "$4" == DE-FILE && "$5" == V002 && "$6" == N ]]
[[ "$8" == NULL && "$9" == NULL && "${10}" == NULL && "${11}" == NULL ]]
for name in "$3" "$7"; do
  [[ -n "$name" && "$name" != */* && "$name" != . && "$name" != .. && "$name" != -* ]]
done
[[ -r "$3" && -s "$3" ]]
# Dummy response only. Input data is mounted read-only.
response_dir="${PWC_DEMO_RESPONSE_DIR:-/pwrcard/home/usr/responses}"
set -o noclobber
printf 'SIMULATION_ONLY\nDATE=%s\nINPUT=%s\nNO_TRANSACTIONS_POSTED\n' "$2" "$3" > "$response_dir/$7"
echo "SIMULATION_ONLY: response created at $response_dir/$7"
