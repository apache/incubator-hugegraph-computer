# AGENTS.md

This file provides guidance to AI coding assistants when working with code in this repository.

## Repository Overview

This is the Apache HugeGraph-Computer repository containing two distinct graph computing systems:

1. **computer** (Java/Maven): A distributed BSP/Pregel-style graph processing framework that runs on Kubernetes or YARN
2. **vermeer** (Go): A high-performance in-memory graph computing platform with master-worker architecture

Both integrate with HugeGraph for graph data input/output.

## Build & Test Commands

### Computer (Java)

**Prerequisites:**
- JDK 11 for building/running
- JDK 8 for HDFS dependencies
- Maven 3.5+
- For K8s module: run `mvn clean install` first to generate CRD classes under computer-k8s

**Build:**
```bash
cd computer
mvn clean compile -Dmaven.javadoc.skip=true
```

**Tests:**
```bash
# Unit tests
mvn test -P unit-test

# Integration tests
mvn test -P integrate-test
```

**Run single test:**
```bash
# Run specific test class
mvn test -P unit-test -Dtest=ClassName

# Run specific test method
mvn test -P unit-test -Dtest=ClassName#methodName
```

**License check:**
```bash
mvn apache-rat:check
```

**Package:**
```bash
mvn clean package -DskipTests
```

### Vermeer (Go)

**Prerequisites:**
- Go 1.23+
- `curl` and `unzip` (for downloading binary dependencies)

**First-time setup:**
```bash
cd vermeer
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

**Regenerate protobuf (if proto files changed):**
```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28.0
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0

# Generate (adjust protoc path for your platform)
vermeer/tools/protoc/linux64/protoc vermeer/apps/protos/*.proto --go-grpc_out=vermeer/apps/protos/. --go_out=vermeer/apps/protos/. # please note remove license header if any
```

## Architecture

### Computer (Java) - BSP/Pregel Framework

**Module Structure:**
- `computer-api`: Public interfaces for graph processing (Computation, Vertex, Edge, Aggregator, Combiner, GraphFactory)
- `computer-core`: Runtime implementation (WorkerService, MasterService, messaging, BSP coordination, managers)
- `computer-algorithm`: Built-in algorithms (PageRank, LPA, WCC, SSSP, TriangleCount, etc.)
- `computer-driver`: Job submission and driver-side coordination
- `computer-k8s`: Kubernetes deployment integration
- `computer-yarn`: YARN deployment integration
- `computer-k8s-operator`: Kubernetes operator for job management
- `computer-dist`: Distribution packaging
- `computer-test`: Integration and unit tests

**Key Design Patterns:**

1. **API/Implementation Separation**: Algorithms depend only on `computer-api` interfaces; `computer-core` provides runtime implementation. Algorithms are dynamically loaded via config.

2. **Manager Pattern**: `WorkerService` composes multiple managers (MessageSendManager, MessageRecvManager, WorkerAggrManager, DataServerManager, SortManagers, SnapshotManager, etc.) with lifecycle hooks: `initAll()`, `beforeSuperstep()`, `afterSuperstep()`, `closeAll()`.

3. **BSP Coordination**: Explicit barrier synchronization via etcd (EtcdBspClient). Each superstep follows:
   - `workerStepPrepareDone` → `waitMasterStepPrepareDone`
   - Local compute (vertices process messages)
   - `workerStepComputeDone` → `waitMasterStepComputeDone`
   - Aggregators/snapshots
   - `workerStepDone` → `waitMasterStepDone` (master returns SuperstepStat)

4. **Computation Contract**: Algorithms implement `Computation<M extends Value>`:
   - `compute0(context, vertex)`: Initialize at superstep 0
   - `compute(context, vertex, messages)`: Process messages in subsequent supersteps
   - Access to aggregators, combiners, and message sending via `ComputationContext`

**Important Files:**
- Algorithm contract: `computer/computer-api/src/main/java/org/apache/hugegraph/computer/core/worker/Computation.java`
- Runtime orchestration: `computer/computer-core/src/main/java/org/apache/hugegraph/computer/core/worker/WorkerService.java`
- BSP coordination: `computer/computer-core/src/main/java/org/apache/hugegraph/computer/core/bsp/Bsp4Worker.java`
- Example algorithm: `computer/computer-algorithm/src/main/java/org/apache/hugegraph/computer/algorithm/centrality/pagerank/PageRank.java`

### Vermeer (Go) - In-Memory Computing Engine

**Directory Structure:**
- `algorithms/`: Go algorithm implementations (pagerank.go, sssp.go, louvain.go, etc.)
- `apps/`:
  - `bsp/`: BSP coordination helpers
  - `graphio/`: HugeGraph I/O adapters (reads via gRPC to store/pd, writes via HTTP REST)
  - `master/`: Master scheduling, HTTP endpoints, worker management
  - `compute/`: Worker-side compute logic
  - `protos/`: Generated protobuf/gRPC definitions
  - `common/`: Utilities, logging, metrics
- `client/`: Client libraries
- `tools/`: Binary dependencies (supervisord, protoc)
- `ui/`: Web UI assets

**Key Patterns:**

1. **Maker/Registry Pattern**: Graph loaders/writers register themselves via init() (e.g., `LoadMakers[LoadTypeHugegraph] = &HugegraphMaker{}`). Master selects loader by type.

2. **HugeGraph Integration**:
   - `hugegraph.go` implements HugegraphMaker, HugegraphLoader, HugegraphWriter
   - Queries PD via gRPC for partition metadata
   - Streams vertex/edge data via gRPC from store (ScanPartition)
   - Writes results back via HugeGraph HTTP REST API

3. **Master-Worker**: Master schedules LoadPartition tasks to workers, manages worker lifecycle via WorkerManager/WorkerClient, exposes HTTP admin endpoints.

**Important Files:**
- HugeGraph integration: `vermeer/apps/graphio/hugegraph.go`
- Master scheduling: `vermeer/apps/master/tasks/tasks.go`
- Worker management: `vermeer/apps/master/workers/workers.go`
- HTTP endpoints: `vermeer/apps/master/services/http_master.go`
- Scheduler: `vermeer/apps/master/bl/scheduler_bl.go`

## Integration with HugeGraph

**Computer (Java):**
- `WorkerInputManager` reads vertices/edges from HugeGraph via `GraphFactory` abstraction
- Graph data is partitioned and distributed to workers via input splits

**Vermeer (Go):**
- Directly queries HugeGraph PD (metadata service) for partition information
- Uses gRPC to stream graph data from HugeGraph store
- Writes computed results back via HugeGraph HTTP REST API (adds properties to vertices)

## Development Workflow

**Adding a New Algorithm (Computer):**
1. Create class in `computer-algorithm` implementing `Computation<MessageType>`
2. Implement `compute0()` for initialization and `compute()` for message processing
3. Use `context.sendMessage()` or `context.sendMessageToAllEdges()` for message passing
4. Register aggregators in `beforeSuperstep()`, read/write in `compute()`
5. Configure algorithm class name in job config

**K8s-Operator Development:**
- CRD classes are auto-generated; run `mvn clean install` in `computer-k8s-operator` first
- Generated classes appear in `computer-k8s/target/generated-sources/`
- CRD generation script: `computer-k8s-operator/crd-generate/Makefile`

**Vermeer Asset Updates:**
- Web UI assets must be regenerated after changes: `cd asset && go generate`
- Or use `make generate-assets` from vermeer root
- For dev mode with hot-reload: `go build -tags=dev`

## Testing Notes

**Computer:**
- Integration tests require etcd, HDFS, HugeGraph, and Kubernetes (see `.github/workflows/computer-ci.yml`)
- Test environment setup scripts in `computer-dist/src/assembly/travis/`
- Unit tests run in isolation without external dependencies

**Vermeer:**
- Test scripts in `vermeer/test/`,with `vermeer_test.go` and `vermeer_test.sh`
- Configuration files in `vermeer/config/` (master.ini, worker.ini templates)

## CI/CD

CI pipeline (`.github/workflows/computer-ci.yml`) runs:
1. License check (Apache RAT)
2. Setup HDFS (Hadoop 3.3.2)
3. Setup Minikube/Kubernetes
4. Load test data into HugeGraph
5. Compile with Java 11
6. Run integration tests (`-P integrate-test`)
7. Run unit tests (`-P unit-test`)
8. Upload coverage to Codecov

## Important Notes

- **Computer K8s module**: Must run `mvn clean install` before editing to generate CRD classes
- **Java version**: Build requires JDK 11; HDFS dependencies require JDK 8
- **Vermeer binary deps**: First-time builds need `make init` to download supervisord/protoc
- **BSP coordination**: Computer uses etcd for barrier synchronization (configure via `BSP_ETCD_URL`)
- **Memory management**: Both systems auto-manage memory by spilling to disk when needed
