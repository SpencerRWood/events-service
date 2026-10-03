#!/usr/bin/env bash
set -euo pipefail

runtime_env="${1:-dev}"
runtime_service="${2:-notify}"
case "$runtime_service" in
  broker) runtime_port=8000 ;;
  notify) runtime_port=8001 ;;
  *) echo "Usage: bash scripts/run_service.sh [environment] [broker|notify]" >&2; exit 2 ;;
esac

exec infisical run --env="$runtime_env" --path=/events-service -- \
  uv run uvicorn "events_service.${runtime_service}:app" \
  --host 127.0.0.1 --port "$runtime_port" --no-access-log
