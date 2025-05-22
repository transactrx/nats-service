#!/bin/sh

# Exit immediately if a command exits with a non-zero status.
set -e

# Run tests using docker-compose configuration
docker-compose up --abort-on-container-exit --exit-code-from test
