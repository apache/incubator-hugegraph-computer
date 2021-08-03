# hugegraph-computer

[![License](https://img.shields.io/badge/license-Apache%202-0E78BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Build Status](https://github.com/hugegraph/hugegraph-computer/actions/workflows/ci.yml/badge.svg)](https://github.com/hugegraph/hugegraph-computer/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/hugegraph/hugegraph-computer/branch/master/graph/badge.svg)](https://codecov.io/gh/hugegraph/hugegraph-computer)

hugegraph-computer is a distributed graph processing system for hugegraph. It is an implementaion of [Pregel](https://kowshik.github.io/JPregel/pregel_paper.pdf). It runs on Kubernetes or YARN framework.

## Features

- Based on BSP(Bulk Synchronous Parallel) model, every iteration is a superstep.
- Auto memory management. The framework will spilt some data to disk, the framework will never OOM(Out of Memory).
- The the part of edges or the messages of super node can be in memory, so you will never loss it.
- You can output the result to HDFS or HugeGraph, or any other system.
- Easy to develop a new algotirhm. You need to focus on a vertex only, not to worry about messages transfering and memory.

## Learn More

The [project homepage](https://hugegraph.github.io/hugegraph-doc/) contains more information about hugegraph-computer. 

## License

hugegraph-computer is licensed under Apache 2.0 License.
