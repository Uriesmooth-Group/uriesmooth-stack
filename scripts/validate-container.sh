#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "$0")/.."

echo "========== 1. DOCKER AVAILABILITY =========="

if ! command -v docker >/dev/null 2>&1; then
    echo "Docker is not installed or not in PATH."
    exit 1
fi

docker --version
docker compose version

echo
echo "========== 2. COMPOSE CONFIGURATION =========="

docker compose config --quiet

echo "Compose configuration is valid."

echo
echo "========== 3. BUILD IMAGE =========="

docker compose build worker

echo
echo "========== 4. DATABASE HEALTH SMOKE TEST =========="

docker compose run --rm --no-deps worker health

echo
echo "========== 5. APPLICATION DISCOVERY SMOKE TEST =========="

docker compose run --rm --no-deps worker list

echo
echo "========== VALIDATION PASSED =========="
echo "The permanent worker service has not been started."
