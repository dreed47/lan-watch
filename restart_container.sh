#!/bin/sh
# Rebuild the image and restart via docker compose.
# Usage:  ./restart_container.sh            (incremental rebuild)
#         ./restart_container.sh --no-cache  (full clean rebuild)
set -e
WD="$(cd "$(dirname "$0")" && pwd)"

BUILD_ARGS=""
if [ "${1:-}" = "--no-cache" ]; then
  BUILD_ARGS="--no-cache"
fi

docker compose -f "${WD}/docker-compose.yml" down --remove-orphans

# shellcheck disable=SC2086
docker compose -f "${WD}/docker-compose.yml" build ${BUILD_ARGS}
docker compose -f "${WD}/docker-compose.yml" up -d

echo "Done — container restarted"
