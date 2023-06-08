# Apache HugeGraph-Computer

[![License](https://img.shields.io/badge/license-Apache%202-0E78BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Build Status](https://github.com/apache/hugegraph-computer/actions/workflows/ci.yml/badge.svg)](https://github.com/apache/hugegraph-computer/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/apache/incubator-hugegraph-computer/branch/master/graph/badge.svg)](https://codecov.io/gh/apache/incubator-hugegraph-computer)
[![Docker Pulls](https://img.shields.io/docker/pulls/hugegraph/hugegraph-computer)](https://hub.docker.com/repository/docker/hugegraph/hugegraph-computer)

The hugegraph-computer is a distributed graph processing system for hugegraph. It is an implementation of [Pregel](https://kowshik.github.io/JPregel/pregel_paper.pdf). It runs on Kubernetes or YARN framework.

## Features

- Support distributed MPP graph computing, and integrates with HugeGraph as graph input/output storage.
- Based on BSP(Bulk Synchronous Parallel) model, an algorithm performs computing through multiple parallel iterations, every iteration is a superstep.
- Auto memory management. The framework will never be OOM(Out of Memory) since it will split some data to disk if it doesn't have enough memory to hold all the data.
- The part of edges or the messages of super node can be in memory, so you will never lose it.
- You can load the data from HDFS or HugeGraph, output the results to HDFS or HugeGraph, or adapt any other systems manually as needed.
- Easy to develop a new algorithm. You just need to focus on a vertex only processing just like as in a single server, without worrying about message transfer and memory/storage management.

## Learn More

The [project homepage](https://hugegraph.apache.org/docs/) contains more information about hugegraph-computer.

And here are links of other repositories:
1. [hugegraph-server](https://github.com/apache/hugegraph) (graph's core component - OLTP server)
2. [hugegraph-toolchain](https://github.com/apache/hugegraph-toolchain) (include loader/dashboard/tool/client)
3. [hugegraph-commons](https://github.com/apache/hugegraph-commons) (include common & rpc module)
4. [hugegraph-website](https://github.com/apache/hugegraph-doc) (include doc & website code)

## Note

- If some classes under computer-k8s cannot be found, you need to execute `mvn clean install` in advance to generate corresponding classes.

## Contributing

Welcome to contribute, please see [`How to Contribute`](https://github.com/apache/hugegraph/blob/master/CONTRIBUTING.md) for more information

Note: It's recommended to use [GitHub Desktop](https://desktop.github.com/) to **greatly simplify** the PR and commit process.

## License

hugegraph-computer is licensed under Apache 2.0 License.

### Contact Us

---

 - [GitHub Issues](https://github.com/apache/incubator-hugegraph-computer/issues): Feedback on usage issues and functional requirements (quick response)
 - Feedback Email: [dev@hugegraph.apache.org](mailto:dev@hugegraph.apache.org) ([subscriber](https://hugegraph.apache.org/docs/contribution-guidelines/subscribe/) only)
 - WeChat public account: Apache HugeGraph, welcome to scan this QR code to follow us.

 <img src="https://github.com/apache/incubator-hugegraph-doc/blob/master/assets/images/wechat.png?raw=true" alt="QR png" width="350"/>

