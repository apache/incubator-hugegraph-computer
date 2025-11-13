# Vermeer Graph Compute Engine

## Introduction
Vermeer is a high-performance distributed graph computing platform based on memory, supporting more than 15 graph algorithms, custom algorithm extensions, and custom data source access.

## Run with Docker

Pull the image:
```
docker pull hugegraph/vermeer:latest
```

Create local configuration files, for example, `~/master.ini` and `~/worker.ini`.

Run with Docker. The `--env` flag specifies the file name.

```
master: docker run -v ~/:/go/bin/config hugegraph/vermeer --env=master
worker: docker run -v ~/:/go/bin/config hugegraph/vermeer --env=worker
```

We've also provided a `docker-compose` file. Once you've created `~/master.ini` and `~/worker.ini`, and updated the `master_peer` in `worker.ini` to `172.20.0.10:6689`, you can run it using the following command:

```
docker-compose up -d
```

## Start

```
master: ./vermeer --env=master
worker: ./vermeer --env=worker01
```
The parameter env specifies the name of the configuration file in the useconfig folder.

```
./vermeer.sh start master
./vermeer.sh start worker
```
Configuration items are specified in vermeer.sh
## supervisord
Can be used with supervisord to start and stop services, automatically start applications, rotate logs, and more; for the configuration file, refer to config/supervisor.conf;

Configuration file reference config/supervisor.conf

````
# run as daemon
./supervisord -c supervisor.conf -d
````

## Build from Source

### Requirements
* Go 1.23 or later
* `curl` and `unzip` utilities (for downloading dependencies)
* Internet connection (for first-time setup)

### Quick Start

**Recommended**: Use Makefile for building:

```bash
# First time setup (downloads binary dependencies)
make init

# Build vermeer
make
```

**Alternative**: Use the build script:

```bash
# For AMD64
./build.sh amd64

# For ARM64
./build.sh arm64
```

# The script will:
# - Auto-detect your OS and architecture if no parameter is provided
# - Download required tools if not present
# - Generate assets and build the binary
# - Exit with error message if any step fails

### Build Targets

```bash
make build              # Build for current platform
make build-linux-amd64  # Build for Linux AMD64
make build-linux-arm64  # Build for Linux ARM64
make clean              # Clean generated files
make help               # Show all available targets
```

### Development Build

For development with hot-reload of web UI:

```bash
go build -tags=dev
```

---

### Protobuf Development

If you need to regenerate protobuf files:

```bash
# Install protobuf Go plugins
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28.0
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0

# Generate protobuf files
tools/protoc/osxm1/protoc *.proto --go-grpc_out=. --go_out=.
```

---






