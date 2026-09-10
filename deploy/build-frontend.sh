#!/usr/bin/env bash
# Build the React frontend for production (same-origin /api via nginx).
# Output: frontend/dist (served by nginx)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT/frontend"

echo "installing frontend deps..."
npm ci --no-audit --no-fund

echo "building production bundle (VITE_API_URL=/api)..."
VITE_API_URL=/api npx vite build

echo "done: $ROOT/frontend/dist"