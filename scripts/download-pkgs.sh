#!/usr/bin/env bash
set -euo pipefail
project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "$project_dir/pkgs"
docker run --rm --user "$(id -u):0" \
  --entrypoint bash \
  -v "$project_dir:/project" \
  apache/airflow:3.1.7-python3.12 -euc '
    export PIP_CACHE_DIR=/tmp/pip-wheel-cache
    python -c '\''from urllib.request import urlretrieve; urlretrieve("https://raw.githubusercontent.com/apache/airflow/constraints-3.1.7/constraints-3.12.txt", "/project/pkgs/constraints-3.12.txt")'\''
    python -m pip download --only-binary=:all: --dest /project/pkgs \
      --constraint /project/pkgs/constraints-3.12.txt \
      "apache-airflow==3.1.7" -r /project/requirements.txt
    python -c '\''from pathlib import Path; from pip._vendor.packaging.utils import parse_wheel_filename; p=Path("/project/pkgs"); pins=sorted({f"{name}=={version}" for f in p.glob("*.whl") for name,version,_,_ in [parse_wheel_filename(f.name)]}); (p/"requirements.lock.txt").write_text("\n".join(pins)+"\n")'\''
  '
