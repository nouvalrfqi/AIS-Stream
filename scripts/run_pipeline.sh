#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
VENV="$ROOT/venv/bin"

run() {
  local name="$1" module="$2"
  if pgrep -f " -m $module" >/dev/null 2>&1; then
    echo "[skip] $name sudah berjalan"
    return
  fi
  [ -f "/tmp/$name.log" ] && cp "/tmp/$name.log" "/tmp/$name.log.prev" 2>/dev/null || true
  nohup "$VENV/python" -m "$module" > "/tmp/$name.log" 2>&1 &
  echo "[start] $name pid=$!"
}

run ingestion ingestion.main
run stream_processor stream_processor.main
run cleanup cleanup.main
run raw_sink raw_sink.main

echo "--- status ---"
pgrep -fl "ingestion.main|stream_processor.main|cleanup.main|raw_sink.main"