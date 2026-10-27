#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
command -v python3 >/dev/null || { echo 'Install Python 3.10+ with your Linux package manager first.' >&2; exit 1; }
python3 -c 'import sys; assert sys.version_info >= (3,10), "Python 3.10+ required"'
mkdir -p input
exec python3 dashboard.py "$@"
