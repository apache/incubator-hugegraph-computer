# Apache HugeGraph-Computer

The hugegraph-computer is a distributed graph processing system for hugegraph. It is an implementation of [Pregel](https://kowshik.github.io/JPregel/pregel_paper.pdf). It runs on Kubernetes or YARN framework.

## Features

- Support distributed MPP graph computing, and integrates with HugeGraph as graph input/output storage.
- Based on BSP(Bulk Synchronous Parallel) model, an algorithm performs computing through multiple parallel iterations, every iteration is a superstep.
- Auto memory management. The framework will never be OOM(Out of Memory) since it will split some data to disk if it doesn't have enough memory to hold all the data.
- The part of edges or the messages of super node can be in memory, so you will never lose it.
- You can load the data from HDFS or HugeGraph, output the results to HDFS or HugeGraph, or adapt any other systems manually as needed.
- Easy to develop a new algorithm. You just need to focus on a vertex only processing just like as in a single server, without worrying about message transfer and memory/storage management.