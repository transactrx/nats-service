# Installing nats-discover CLI

`nats-discover` is a command-line tool for discovering NATS services built with the nats-service framework. It allows you to list all running services, view their API documentation, and monitor endpoint statistics.

## macOS

### Homebrew (Recommended)

```bash
# Add the tap
brew tap transactrx/tap

# Install
brew install nats-discover

# Verify installation
nats-discover --version
```

### Manual Installation

```bash
# Download the latest release for your architecture
# For Apple Silicon (M1/M2/M3):
curl -L https://github.com/transactrx/nats-service/releases/latest/download/nats-discover_darwin_arm64.tar.gz | tar xz

# For Intel Macs:
curl -L https://github.com/transactrx/nats-service/releases/latest/download/nats-discover_darwin_amd64.tar.gz | tar xz

# Move to a directory in your PATH
sudo mv nats-discover /usr/local/bin/

# Verify installation
nats-discover --version
```

## Linux

### Manual Installation

```bash
# Download the latest release for your architecture
# For x86_64 (most common):
curl -L https://github.com/transactrx/nats-service/releases/latest/download/nats-discover_linux_amd64.tar.gz | tar xz

# For ARM64 (e.g., Raspberry Pi 4, AWS Graviton):
curl -L https://github.com/transactrx/nats-service/releases/latest/download/nats-discover_linux_arm64.tar.gz | tar xz

# Move to a directory in your PATH
sudo mv nats-discover /usr/local/bin/

# Verify installation
nats-discover --version
```

### Using Go

If you have Go installed:

```bash
go install github.com/transactrx/nats-service/cmd/nats-discover@latest
```

## Windows

### Scoop (Recommended)

```powershell
# Add the bucket
scoop bucket add transactrx https://github.com/transactrx/scoop-bucket

# Install
scoop install nats-discover

# Verify installation
nats-discover --version
```

### Manual Installation

1. Download the latest release from [GitHub Releases](https://github.com/transactrx/nats-service/releases/latest)
   - For 64-bit Windows: `nats-discover_windows_amd64.zip`
   - For ARM64 Windows: `nats-discover_windows_arm64.zip`

2. Extract the ZIP file

3. Move `nats-discover.exe` to a directory in your PATH, or add the extraction directory to your PATH

4. Verify installation:
   ```powershell
   nats-discover --version
   ```

### Using Go

If you have Go installed:

```powershell
go install github.com/transactrx/nats-service/cmd/nats-discover@latest
```

## Build from Source

If you prefer to build from source:

```bash
# Clone the repository
git clone https://github.com/transactrx/nats-service.git
cd nats-service

# Build
go build -o nats-discover ./cmd/nats-discover

# Move to your PATH (Linux/macOS)
sudo mv nats-discover /usr/local/bin/
```

## Quick Start

Once installed, you can discover services:

```bash
# List all services (uses NATS context or NATS_URL env var)
nats-discover

# Specify NATS server explicitly
nats-discover -s nats://localhost:4222

# Use a NATS CLI context
nats-discover --context mycontext

# View API docs for a specific service
nats-discover -s nats://localhost:4222 -S myservice.api

# View stats for a service (all instances)
nats-discover -s nats://localhost:4222 -S myservice.api --stats

# Output in JSON format
nats-discover -s nats://localhost:4222 --format json

# Output in YAML format
nats-discover -s nats://localhost:4222 --format yaml
```

## Authentication

`nats-discover` supports various authentication methods:

```bash
# Using credentials file
nats-discover -s nats://server:4222 --creds /path/to/user.creds

# Using NKey
nats-discover -s nats://server:4222 --nkey /path/to/user.nk

# Using JWT and seed
nats-discover -s nats://server:4222 --jwt "eyJ..." --seed "SUAM..."

# Using NATS CLI context (includes auth from context)
nats-discover --context production
```

## Upgrading

### Homebrew (macOS)

```bash
brew update
brew upgrade nats-discover
```

### Scoop (Windows)

```powershell
scoop update nats-discover
```

### Manual

Download the latest release and replace the existing binary.
