#!/bin/sh

# Exit immediately if a command exits with a non-zero status.
set -e

# Wait for NATS to be healthy
# This is a simple loop that tries to connect to NATS.
# Adjust the timeout and attempts as necessary.
echo "Waiting for NATS to start..."
attempts=0
limit=30 # Roughly 30 seconds
until nc -z localhost 4222; do
  attempts=$((attempts+1))
  if [ $attempts -gt $limit ]; then
    echo "NATS did not start in time. Exiting."
    exit 1
  fi
  sleep 1
done
echo "NATS started."

# Run the Go tests
echo "Running Go tests..."
go test -v ./...

echo "Tests completed."
