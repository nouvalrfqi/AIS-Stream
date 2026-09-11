#!/usr/bin/env bash
# Idempotent installer for maritime-tracking systemd units + optional nginx site.
# Usage:
#   ./install-systemd.sh            # install units + nginx site (requires root/sudo)
#   ./install-systemd.sh --no-nginx # install units only
#   ./install-systemd.sh --uninstall
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SYSTEMD_DIR="${SYSTEMD_DIR:-/etc/systemd/system}"
SRC="$ROOT/deploy/systemd"
ENV_SRC="$ROOT/.env"
ENV_SYSTEMD="$ROOT/.env.systemd"
# RHEL-family nginx (incl. Amazon Linux 2023) only includes /etc/nginx/conf.d/*.conf.
# Debian-family also includes conf.d/*.conf, so conf.d works portably on both.
NGINX_CONFDIR="${NGINX_CONFDIR:-/etc/nginx/conf.d}"
NGINX_SITE="maritime-tracking"

cleanup_legacy_site() {
  # Remove earlier Debian-style sites-enabled layout if present from a prior run.
  rm -f /etc/nginx/sites-enabled/$NGINX_SITE /etc/nginx/sites-available/$NGINX_SITE 2>/dev/null || true
}

uninstall_nginx() {
  rm -f "$NGINX_CONFDIR/$NGINX_SITE.conf"
  cleanup_legacy_site
  if command -v nginx >/dev/null 2>&1; then
    nginx -t >/dev/null 2>&1 && nginx -s reload 2>/dev/null || true
  fi
  echo "removed nginx site: $NGINX_SITE"
}

if [[ "${1:-}" == "--uninstall" ]]; then
  for unit in "$SRC"/*.service; do
    name="$(basename "$unit")"
    rm -f "$SYSTEMD_DIR/$name"
  done
  systemctl daemon-reload 2>/dev/null || true
  uninstall_nginx
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

systemctl daemon-reload 2>/dev/null || true
echo "done. start with:"
echo "  systemctl enable --now maritime-ingestion maritime-stream-processor maritime-raw-sink maritime-cleanup maritime-api"

if [[ "${1:-}" == "--no-nginx" ]]; then
  exit 0
fi

if ! command -v nginx >/dev/null 2>&1; then
  echo "NOTE: nginx not found; skipping site install. Run again after: apt install nginx"
  exit 0
fi

if [[ "$ROOT" == *" "* ]]; then
  echo "WARNING: project path contains spaces; nginx root cannot be quoted (quoted root + try_files = redirection cycle)." >&2
  echo "         Deploy from a space-free path (e.g. /opt/maritim-tracking), then re-run to install the nginx site." >&2
  exit 0
fi

install -d "$NGINX_CONFDIR"
cleanup_legacy_site
sed "s|<ROOT>|$ROOT|g" "$ROOT/deploy/nginx.conf" > "$NGINX_CONFDIR/$NGINX_SITE.conf"
chmod 644 "$NGINX_CONFDIR/$NGINX_SITE.conf"
if nginx -t; then
  nginx -s reload 2>/dev/null || true
  echo "nginx site installed + reloaded: $NGINX_CONFDIR/$NGINX_SITE.conf"
else
  echo "ERROR: nginx -t failed; site config saved but NOT reloaded" >&2
fi