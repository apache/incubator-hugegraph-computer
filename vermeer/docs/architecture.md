# Vermeer Architectural Design

## Running architecture

Vermeer has two roles: master and worker. There is only one master, but there can be multiple workers. The master is responsible for communication, forwarding, and summarizing, with a small amount of computation and low resource usage; the workers are computational nodes used for storing graph data and running computation tasks, consuming a large amount of memory and CPU. gRPC is used for internal communication, while REST is used for external calls.

```mermaid
graph TD
    subgraph Master
        A[HTTP handler] --> B[Business logic]
        B --> C1[Task Manager]
        B --> C2[Graph Manager]
        B --> C3[Worker Manager]
        C1 --> D[gRPC handler]
        C2 --> D
        C3 --> D
    end
    subgraph Worker1
        E[gRPC handler] --> F[Business logic]
        F --> G1[Task Manager]
        F --> G2[Graph Manager]
        F --> G3[Peer Manager]
    end
    subgraph Worker2
        H[gRPC handler] --> I[Business logic]
        I --> J1[Task Manager]
        I --> J2[Graph Manager]
        I --> J3[Peer Manager]
    end
    request --> Master
    D --> E
    D --> H
```

## Load process

```mermaid
sequenceDiagram
    participant User
    participant master
    participant worker
    participant DataSource

    User->>master: User initiates import request
    master->>worker: Notify worker to start importing vertices
    worker->>master: Get LoadPartition (vertex)
    worker->>DataSource: Get data
    worker->>worker: Send to corresponding worker by vertex hash
    worker->>worker: scatter vertex
    worker->>worker: gather vertex
    master->>worker: Notify worker to start importing edges
    worker->>master: Get LoadPartition (edge)
    worker->>DataSource: Get data
    worker->>worker: Send to corresponding worker by vertex hash
```

## Computing process

```mermaid
sequenceDiagram
    participant User
    participant master
    participant worker1 as worker
    participant worker2 as worker

    User->>master: User initiates compute request
    master->>worker1: Notify worker to start computing
    master->>worker2: Notify worker to start computing
    worker1->>worker1: Init
    worker1->>worker1: BeforeStep
    worker1->>worker1: Compute
    worker1->>master: Scatter vertex values to mirror vertices
    worker2->>worker2: Init
    worker2->>worker2: BeforeStep
    worker2->>worker2: Compute
    worker2->>master: Scatter vertex values to mirror vertices
    worker1->>master: aggregate
    worker2->>master: aggregate
    master->>master: Master Compute determines whether to continue
    master->>worker1: next step or output
    master->>worker2: next step or output
```

