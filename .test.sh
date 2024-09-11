#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="schema-registry"

# Loop until the service is running
while true; do
  # Check if the service is running
  if [ -z $(docker compose -f tests/docker-compose.yml ps -q $SERVICE_NAME) ] || [ -z $(docker ps -q --no-trunc | grep $(docker compose ps -q $SERVICE_NAME)) ]; then
    echo "No, $SERVICE_NAME is not running."
    sleep 5
  else
    echo "Yes, $SERVICE_NAME is running."
    break
  fi
done


python -m mypy .
python -m pytest -vv
