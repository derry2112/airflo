#!/usr/bin/env bash
set -euo pipefail

# Local runner: preview by default; --execute runs the supplied PWC script.
PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="${PWC_DATA_DIR:-${PROJECT_DIR}/data/pwrcard/home/usr/data}"
PWC_SCRIPT="${PWC_SCRIPT:-}"
execute=false

usage() {
  cat <<'EOF'
Usage: bash scripts/run_pwc_posting.sh [--execute] FILENAME RESPONSE_FILENAME

Default: preview only. Files are resolved inside local data/pwrcard/home/usr/data.
Arguments must be basenames, without directory components.

Environment:
  PWC_DATA_DIR  Override the local data directory.
  PWC_SCRIPT    Absolute path to the real PWC .sh script (required for --execute).

Example preview:
  bash scripts/run_pwc_posting.sh POSTFLIN_example.txt RESPONSE_example.txt

Example execution:
  PWC_SCRIPT=/absolute/path/PWC.sh bash scripts/run_pwc_posting.sh --execute POSTFLIN_example.txt RESPONSE_example.txt

The date is today's date in Asia/Jakarta, formatted DDMMYYYY.
Execution runs in the data directory. Re-running may process the same file again.
EOF
}

if [[ "${1:-}" == '--help' || "${1:-}" == '-h' ]]; then
  usage
  exit 0
fi
if [[ "${1:-}" == '--execute' ]]; then
  execute=true
  shift
fi
if [[ $# -ne 2 ]]; then
  usage >&2
  exit 2
fi
filename="$1"
response_filename="$2"
for name in "$filename" "$response_filename"; do
  if [[ -z "$name" || "$name" == */* || "$name" == '.' || "$name" == '..' || "$name" == -* ]]; then
    echo 'FILENAME dan RESPONSE_FILENAME harus berupa nama file tanpa direktori.' >&2
    exit 2
  fi
done
if [[ "$filename" == "$response_filename" ]]; then
  echo 'Nama response harus berbeda dari file input.' >&2
  exit 2
fi
cd -- "$DATA_DIR"
if [[ ! -f "$filename" || ! -r "$filename" || ! -s "$filename" ]]; then
  echo "File input tidak ditemukan, kosong, atau tidak dapat dibaca: $filename" >&2
  exit 1
fi
if [[ -e "$response_filename" || -L "$response_filename" ]]; then
  echo "File response sudah ada; gunakan nama lain: $response_filename" >&2
  exit 1
fi
posting_date="$(TZ=Asia/Jakarta date +%d%m%Y)"
args=(load_posting_new_file "$posting_date" "$filename" DE-FILE V002 N "$response_filename" NULL NULL NULL NULL)
printf 'Working directory: %s\n' "$PWD"
printf 'Command: '
printf '%q ' bash "${PWC_SCRIPT:-<PATH_SCRIPT_PWC.sh>}" "${args[@]}"
printf '\n'
if [[ "$execute" == false ]]; then
  echo 'Preview saja; belum ada script PWC yang dijalankan.'
  exit 0
fi
if [[ "$PWC_SCRIPT" != /* || ! -f "$PWC_SCRIPT" || ! -r "$PWC_SCRIPT" ]]; then
  echo 'Isi PWC_SCRIPT dengan path absolut script PWC yang dapat dibaca.' >&2
  exit 2
fi
exec bash "$PWC_SCRIPT" "${args[@]}"
