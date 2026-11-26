#!/bin/sh
# Shared entry point for every compose group.
# - Creates examples/.env from .env.template on first use.
# - Rewrites the host loopback proxy to host.docker.internal for OrbStack.
# - Forces --env-file so all groups read the same examples/.env.
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ENV_FILE="$ROOT/.env"
if [ ! -f "$ENV_FILE" ]; then
  cp "$ROOT/.env.template" "$ENV_FILE"
else
  # Keep operator edits, but add keys introduced by a newer template.
  while IFS= read -r line; do
    case "$line" in
      ''|'#'*) continue ;;
      *=*)
        key=${line%%=*}
        if ! grep -q "^${key}=" "$ENV_FILE"; then
          printf '%s\n' "$line" >> "$ENV_FILE"
        fi
        ;;
    esac
  done < "$ROOT/.env.template"
fi

proxy="${AOA_EXAMPLE_PROXY:-}"
if [ -z "$proxy" ]; then
  proxy="${HTTPS_PROXY:-${HTTP_PROXY:-}}"
  if [ -n "$proxy" ]; then
    proxy=$(printf '%s' "$proxy" | sed \
      -e 's/127\.0\.0\.1/host.docker.internal/g' \
      -e 's/localhost/host.docker.internal/g')
  fi
fi
export AOA_EXAMPLE_PROXY="$proxy"

exec docker compose --env-file "$ENV_FILE" "$@"
