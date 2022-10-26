# hugegraph-computer

[![License](https://img.shields.io/badge/license-Apache%202-0E78BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Build Status](https://github.com/hugegraph/hugegraph-computer/actions/workflows/ci.yml/badge.svg)](https://github.com/hugegraph/hugegraph-computer/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/hugegraph/hugegraph-computer/branch/master/graph/badge.svg)](https://codecov.io/gh/hugegraph/hugegraph-computer)
[![Docker Pulls](https://img.shields.io/docker/pulls/hugegraph/hugegraph-builtin-algorithms)](https://hub.docker.com/repository/docker/hugegraph/hugegraph-builtin-algorithms)

hugegraph-computer is a distributed graph processing system for hugegraph. It is an implementation of [Pregel](https://kowshik.github.io/JPregel/pregel_paper.pdf). It runs on Kubernetes or YARN framework.

## Features

- Based on BSP(Bulk Synchronous Parallel) model, every iteration is a superstep.
- Auto memory management. The framework will split some data to disk, the framework will never OOM(Out of Memory).
- The part of edges or the messages of super node can be in memory, so you will never lose it.
- You can output the result to HDFS or HugeGraph, or any other system.
- Easy to develop a new algorithm. You need to focus on a vertex only, not to worry about messages transferring and memory.

## Learn More

The [project homepage](https://hugegraph.apache.org/docs/) contains more information about hugegraph-computer.

And here are links of other repositories:
1. [hugegraph-server](https://github.com/apache/incubator-hugegraph) (graph's core component - OLTP server)
2. [hugegraph-toolchain](https://github.com/apache/incubator-hugegraph-toolchain) (include loader/dashboard/tool/client)
3. [hugegraph-commons](https://github.com/apache/incubator-hugegraph-commons) (include common & rpc module)
4. [hugegraph-website](https://github.com/apache/incubator-hugegraph-doc) (include doc & website code)

## Contributing

Welcome to contribute, please see [`How to Contribute`](https://github.com/apache/incubator-hugegraph/blob/master/CONTRIBUTING.md) for more information

Note: It's recommended to use [GitHub Desktop](https://desktop.github.com/) to **greatly simplify** the PR and commit process.

## License

hugegraph-computer is licensed under Apache 2.0 License.
