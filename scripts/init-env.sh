#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
python3 - <<'PYTHON'
from pathlib import Path
import os
import secrets
path = Path('.env')
existing = path.read_text() if path.exists() else ''
if not any(line.startswith('AIRFLOW_JWT_SECRET=') and line.partition('=')[2].strip() for line in existing.splitlines()):
    with path.open('a') as env_file:
        if existing and not existing.endswith('\n'):
            env_file.write('\n')
        env_file.write('AIRFLOW_JWT_SECRET=' + secrets.token_hex(32) + '\n')
os.chmod(path, 0o600)
PYTHON
