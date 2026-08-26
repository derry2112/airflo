#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
AIRFLOW_BIN="${PROJECT_DIR}/.venv/bin/airflow"

if [[ ! -x "${AIRFLOW_BIN}" ]]; then
  echo "Airflow belum terinstal. Jalankan ./scripts/setup.sh terlebih dahulu." >&2
  exit 1
fi

export AIRFLOW_HOME="${PROJECT_DIR}"
exec "${AIRFLOW_BIN}" standalone
