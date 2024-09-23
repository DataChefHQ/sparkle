#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="schema-registry"

# Function to check if the container is healthy
check_health() {
    STATUS=$(docker inspect --format='{{.State.Health.Status}}' schema-registry 2>/dev/null)

    if [ "$STATUS" == "healthy" ]; then
        echo "Container 'schema-registry' is healthy."
        return 0
    else
        echo "Container 'schema-registry' is not healthy. Current status: $STATUS"
        echo "Did you remember to start the processes?"
        echo "You can do so by running \`up\` in the terminal."
        echo
        return 1
    fi
}

# Loop until the container is healthy
while true; do
    if check_health; then
        break
    else
        echo "Retrying in 5 seconds..."
        echo
        sleep 5
    fi
done

python -m mypy .
python -m pytest -vv
