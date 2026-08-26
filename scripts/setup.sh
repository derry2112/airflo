#!/usr/bin/env bash
set -euo pipefail

AIRFLOW_VERSION="${AIRFLOW_VERSION:-3.1.7}"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

find_python() {
  if [[ -n "${PYTHON_BIN:-}" ]]; then
    command -v "${PYTHON_BIN}" || return 1
    return
  fi

  local candidate
  for candidate in python3.12 python3.11 python3.10 python3.13; do
    if command -v "${candidate}" >/dev/null 2>&1; then
      command -v "${candidate}"
      return
    fi
  done
  return 1
}

PYTHON_PATH="$(find_python || true)"
if [[ -z "${PYTHON_PATH}" ]]; then
  echo "Error: Airflow ${AIRFLOW_VERSION} memerlukan Python 3.10–3.13." >&2
  echo "Instal Python 3.12, lalu jalankan: PYTHON_BIN=python3.12 make setup" >&2
  exit 1
fi

PYTHON_MINOR="$(${PYTHON_PATH} -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
case "${PYTHON_MINOR}" in
  3.10|3.11|3.12|3.13) ;;
  *)
    echo "Error: ${PYTHON_PATH} menggunakan Python ${PYTHON_MINOR}, yang belum didukung." >&2
    exit 1
    ;;
esac

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_MINOR}.txt"

"${PYTHON_PATH}" -m venv "${PROJECT_DIR}/.venv"
"${PROJECT_DIR}/.venv/bin/python" -m pip install --upgrade pip
"${PROJECT_DIR}/.venv/bin/pip" install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

if grep -Eq '^[[:space:]]*[^#[:space:]]' "${PROJECT_DIR}/requirements.txt"; then
  "${PROJECT_DIR}/.venv/bin/pip" install -r "${PROJECT_DIR}/requirements.txt"
fi

AIRFLOW_HOME="${PROJECT_DIR}" "${PROJECT_DIR}/.venv/bin/airflow" db migrate

echo
echo "Setup selesai. Jalankan Airflow dengan: make up"
