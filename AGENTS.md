# Repository Contribution Guidelines

This repository contains Go modules for working with NATS.

## Development

- Format all Go files with `gofmt -w` before committing.
- Ensure that `go vet ./...` reports no issues.
- Run the full test suite with `go test ./...`.
- Check that `go build ./...` succeeds.

## Pull Requests

Provide a short summary of the changes in your pull request description. Make sure the programmatic checks above pass before opening a PR.

## Running Tests with Docker Compose

To run the comprehensive tests using Docker Compose, ensure you have Docker and Docker Compose installed. Then, from the root of the repository, run:

```bash
docker-compose up --build --abort-on-container-exit
```

This command will:
- Build the application Docker image (if not already built or if Dockerfile has changed).
- Start the NATS service.
- Start the application service and run the tests defined in `run_tests.sh`.
- Stop and remove the containers after the tests complete.

