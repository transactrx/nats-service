# Repository Contribution Guidelines

This repository contains Go modules for working with NATS.

## Development

- Format all Go files with `gofmt -w` before committing.
- Ensure that `go vet ./...` reports no issues.
- Run the full test suite with `go test ./...`.
- Check that `go build ./...` succeeds.

## Pull Requests

Provide a short summary of the changes in your pull request description. Make sure the programmatic checks above pass before opening a PR.

