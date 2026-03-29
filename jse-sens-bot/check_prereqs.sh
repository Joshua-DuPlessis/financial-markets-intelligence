#!/usr/bin/env bash
set -euo pipefail

pass() {
  printf '[OK] %s\n' "$1"
}

warn() {
  printf '[WARN] %s\n' "$1"
}

check_cmd() {
  local cmd="$1"
  local label="$2"
  if command -v "$cmd" >/dev/null 2>&1; then
    pass "$label is installed"
  else
    warn "$label is missing"
  fi
}

check_python_module() {
  local module="$1"
  local label="$2"
  if python3 -c "import ${module}" >/dev/null 2>&1; then
    pass "$label is importable"
  else
    warn "$label is not importable"
  fi
}

check_cmd python3 "Python 3"
check_cmd docker "Docker"
check_cmd nginx "Nginx"
check_python_module playwright "playwright package"
check_python_module requests "requests package"
check_python_module pypdf "pypdf package"

if command -v docker >/dev/null 2>&1; then
  if docker compose version >/dev/null 2>&1; then
    pass "Docker Compose plugin is available"
  else
    warn "Docker Compose plugin is missing"
  fi
fi

printf '\nRun internal tests with: python3 -m unittest discover -s tests -v\n'
