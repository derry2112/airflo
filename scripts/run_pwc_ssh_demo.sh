#!/usr/bin/env bash
set -euo pipefail

# Settings for the local Docker simulation only.
SSH_HOST=127.0.0.1
SSH_PORT=2222
SSH_USER=pwcdemo
# menentukan lokasi skrip PWC.sh di dalam kontainer Docker. Gunakan path absolut.
REMOTE_SCRIPT=/opt/pwc/PWC.sh
# remote data directory di dalam kontainer Docker. Gunakan path absolut.
REMOTE_DATA_DIR=/pwrcard/home/usr/data
# Password is entered at the SSH prompt: demo-local-only

if [[ $# -ne 2 ]]; then
  echo "Usage: bash $0 FILENAME RESPONSE_FILENAME" >&2
  exit 2
fi
for name in "$1" "$2"; do
  [[ -n "$name" && "$name" != */* && "$name" != . && "$name" != .. && "$name" != -* ]] || exit 2
done
posting_date="$(TZ=Asia/Jakarta date +%d%m%Y)"
printf -v command 'cd %q && bash %q load_posting_new_file %q %q DE-FILE V002 N %q NULL NULL NULL NULL' \
  "$REMOTE_DATA_DIR" "$REMOTE_SCRIPT" "$posting_date" "$1" "$2"
exec ssh -p "$SSH_PORT" "$SSH_USER@$SSH_HOST" "$command"
