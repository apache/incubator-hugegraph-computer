# AGENTS.md

This file provides guidance to AI coding assistants when working with code in this repository.

## Repository Overview

Vermeer is a high-performance in-memory graph computing platform written in Go. It features a single-binary deployment model with master-worker architecture, supporting 20+ graph algorithms and seamless HugeGraph integration.

## Build & Test Commands

**Prerequisites:**
- Go 1.23+
- `curl` and `unzip` (for downloading binary dependencies)

**First-time setup:**
```bash
make init  # Downloads supervisord and protoc binaries, installs Go deps
```

**Build:**
```bash
make          # Build for current platform
make build-linux-amd64
make build-linux-arm64
```

**Development build with hot-reload UI:**
```bash
go build -tags=dev
```

**Clean:**
```bash
make clean      # Remove built binaries and generated assets
make clean-all  # Also remove downloaded tools
```

**Run:**
```bash
# Using binary directly
./vermeer --env=master
./vermeer --env=worker

# Using script (configure in vermeer.sh)
./vermeer.sh start master
./vermeer.sh start worker
```

**Tests:**
```bash
# Run with build tag vermeer_test
go test -tags=vermeer_test -v

# Specific test modes
go test -tags=vermeer_test -v -mode=algorithms
go test -tags=vermeer_test -v -mode=function
go test -tags=vermeer_test -v -mode=scheduler
```

**Regenerate protobuf (if proto files changed):**
```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28.0
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0
tools/protoc/osxm1/protoc *.proto --go-grpc_out=. --go_out=.
```

## Architecture

### Directory Structure

```
vermeer/
├── main.go              # Single binary entry point
├── algorithms/          # Algorithm implementations
│   ├── algorithms.go    # AlgorithmMaker registry
│   ├── pagerank.go
│   ├── louvain.go
│   └── ...
├── apps/
│   ├── master/          # Master service
│   │   ├── services/    # HTTP handlers
│   │   ├── workers/     # Worker management (WorkerManager, WorkerClient)
│   │   ├── tasks/       # Task scheduling
│   │   └── graphs/      # Graph metadata management
│   ├── worker/          # Worker service entry
│   ├── compute/         # Worker-side compute logic
│   │   ├── api.go       # Algorithm interface definition
│   │   ├── task.go      # Compute task execution
│   │   └── ...
│   ├── graphio/         # Graph I/O (HugeGraph, CSV, HDFS)
│   │   └── hugegraph.go # HugeGraph integration
│   ├── protos/          # gRPC definitions
│   ├── common/          # Utilities, logging, metrics
│   ├── structure/       # Graph data structures
│   ├── storage/         # Persistence layer
│   └── bsp/             # BSP coordination helpers
├── config/              # Configuration templates
├── tools/               # Binary dependencies (supervisord, protoc)
└── ui/                  # Web dashboard
```

### Key Design Patterns

**1. Maker/Registry Pattern**

Graph loaders and writers register themselves via `init()`:

```go
func init() {
    LoadMakers[LoadTypeHugegraph] = &HugegraphMaker{}
}
```

Master selects loader by type from the registry. Algorithms follow the same pattern in `algorithms/algorithms.go`.

**2. Master-Worker Architecture**

- **Master**: Schedules LoadPartition tasks to workers, manages worker lifecycle via WorkerManager/WorkerClient, exposes HTTP endpoints for graph/task management
- **Worker**: Executes compute tasks, reports status back to master via gRPC
- Communication: Master uses gRPC clients to workers (apps/master/workers/); workers connect to master on startup

**3. HugeGraph Integration**

Implementation in `apps/graphio/hugegraph.go`:

1. **Metadata Query**: Queries HugeGraph PD (metadata service) via gRPC for partition information
2. **Data Loading**: Streams vertices/edges from HugeGraph Store via gRPC (`ScanPartition`)
3. **Result Writing**: Writes computed results back via HugeGraph HTTP REST API (adds vertex properties)

The loader queries PD first (`QueryPartitions`), then creates LoadPartition tasks for each partition, which workers execute by calling `ScanPartition` on store nodes.

**4. Algorithm Interface**

Algorithms implement the interface defined in `apps/compute/api.go`. Each algorithm must register itself in `algorithms/algorithms.go` by appending to the `Algorithms` slice.

**5. Single Binary Entry Point**

`main.go` loads config from `config/{env}.ini`, then starts either master or worker based on `run_mode` parameter. The `--env` flag specifies which config file to use (e.g., `--env=master` loads `config/master.ini`).

## Important Files

- Entry point: `main.go`
- Algorithm interface: `apps/compute/api.go`
- Algorithm registry: `algorithms/algorithms.go`
- HugeGraph integration: `apps/graphio/hugegraph.go`
- Master scheduling: `apps/master/tasks/tasks.go`
- Worker management: `apps/master/workers/workers.go`
- HTTP endpoints: `apps/master/services/http_master.go`

## Development Workflow

**Adding a New Algorithm:**

1. Create file in `algorithms/` implementing the interface from `apps/compute/api.go`
2. Register in `algorithms/algorithms.go` by appending to `Algorithms` slice
3. Implement required methods: `Init()`, `Compute()`, `Aggregate()`, `Terminate()`
4. Rebuild: `make`

**Modifying Web UI:**

1. Edit files in `ui/`
2. Regenerate assets: `cd asset && go generate`
3. Or use dev build: `go build -tags=dev` (hot-reload enabled)

**Modifying Protobuf Definitions:**

1. Edit `.proto` files in `apps/protos/`
2. Regenerate Go code using protoc (adjust path for platform):
   ```bash
   tools/protoc/osxm1/protoc apps/protos/*.proto --go-grpc_out=. --go_out=.
   ```

## Configuration

**Master (`config/master.ini`):**
- `http_peer`: Master HTTP listen address (default: 0.0.0.0:6688)
- `grpc_peer`: Master gRPC listen address (default: 0.0.0.0:6689)
- `run_mode`: Must be "master"
- `task_parallel_num`: Number of parallel tasks

**Worker (`config/worker.ini`):**
- `http_peer`: Worker HTTP listen address (default: 0.0.0.0:6788)
- `grpc_peer`: Worker gRPC listen address (default: 0.0.0.0:6789)
- `master_peer`: Master gRPC address to connect (must match master's `grpc_peer`)
- `run_mode`: Must be "worker"

## Memory Management

Vermeer uses an in-memory-first approach. Graphs are distributed across workers and stored in memory. Ensure total worker memory exceeds graph size by 2-3x for algorithm workspace.

## Testing Notes

Tests require the build tag `vermeer_test`:

```bash
go test -tags=vermeer_test -v
```

Test modes (set via `-mode` flag):
- `algorithms`: Algorithm correctness tests
- `function`: Functional integration tests
- `scheduler`: Scheduler behavior tests

Test configuration via flags:
- `-master`: Master HTTP address
- `-worker01/02/03`: Worker HTTP addresses
- `-auth`: Authentication type
