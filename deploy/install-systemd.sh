#!/usr/bin/env bash
# Idempotent installer for maritime-tracking systemd units.
# Usage:
#   ./install-systemd.sh          # install/symlink units (requires root/sudo)
#   ./install-systemd.sh --uninstall
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SYSTEMD_DIR="${SYSTEMD_DIR:-/etc/systemd/system}"
SRC="$ROOT/deploy/systemd"
ENV_SRC="$ROOT/.env"
ENV_SYSTEMD="$ROOT/.env.systemd"

if [[ "${1:-}" == "--uninstall" ]]; then
  for unit in "$SRC"/*.service; do
    name="$(basename "$unit")"
    rm -f "$SYSTEMD_DIR/$name"
  done
  systemctl daemon-reload 2>/dev/null || true
  echo "units removed; run 'systemctl disable --now <unit>' first if enabled"
  exit 0
fi

[[ -f "$ENV_SRC" ]] || { echo "ERROR: $ENV_SRC not found" >&2; exit 1; }

# systemd EnvironmentFile cannot parse "KEY = value" spacing -> normalize.
sed -E -e 's/^ *([A-Za-z_][A-Za-z0-9_]*) *= */\1=/' "$ENV_SRC" > "$ENV_SYSTEMD"
chmod 600 "$ENV_SYSTEMD"
echo "env file for systemd: $ENV_SYSTEMD"

install -d "$SYSTEMD_DIR"
for unit in "$SRC"/*.service; do
  name="$(basename "$unit")"
  sed "s|<ROOT>|$ROOT|g" "$unit" > "$SYSTEMD_DIR/$name"
  chmod 644 "$SYSTEMD_DIR/$name"
  echo "installed: $SYSTEMD_DIR/$name"
done

systemctl daemon-reload
echo "done. start with:"
echo "  systemctl enable --now maritime-ingestion maritime-stream-processor maritime-raw-sink maritime-cleanup maritime-api"