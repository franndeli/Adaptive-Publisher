#!/usr/bin/env bash

# CONFIG ------------------------------------------------------
# Container to track CPU usage of
CONTAINER=("adaptive-publisher-service-1")

# Log file
LOG_FILE="cpu_usage.log"
# -------------------------------------------------------------

echo "Logging CPU usage every second. Press Ctrl+C to stop."
echo "timestamp,container,cpu_percent" > "$LOG_FILE"

while true; do
    TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")

    # Grab the full stats table once
    STATS=$(docker compose stats --no-stream)

    # Extract the CPU % column for this container
    CPU=$(echo "$STATS" | awk -v target="$CONTAINER" '$2 == target {print $3}')

    # Print to terminal
    echo "$TIMESTAMP  $CONTAINER  CPU: $CPU"

    # Append to log
    echo "$TIMESTAMP,$CONTAINER,$CPU" >> "$LOG_FILE"
done
