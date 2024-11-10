# Apache HugeGraph-Computer

[![License](https://img.shields.io/badge/license-Apache%202-0E78BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Build Status](https://github.com/apache/hugegraph-computer/actions/workflows/ci.yml/badge.svg)](https://github.com/apache/hugegraph-computer/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/apache/incubator-hugegraph-computer/branch/master/graph/badge.svg)](https://codecov.io/gh/apache/incubator-hugegraph-computer)
[![Docker Pulls](https://img.shields.io/docker/pulls/hugegraph/hugegraph-computer)](https://hub.docker.com/repository/docker/hugegraph/hugegraph-computer)

The [hugegraph-computer](./computer/README.md) is a distributed graph processing system for hugegraph. 
(Also, the in-memory computing engine(vermeer) is on the way 🚧)

## Learn More

The [project homepage](https://hugegraph.apache.org/docs/quickstart/hugegraph-computer/) contains more information about hugegraph-computer.

And here are links of other repositories:
1. [hugegraph](https://github.com/apache/hugegraph) (graph's core component - Graph server + PD + Store)
2. [hugegraph-toolchain](https://github.com/apache/hugegraph-toolchain) (graph tools **[loader](https://github.com/apache/incubator-hugegraph-toolchain/tree/master/hugegraph-loader)/[dashboard](https://github.com/apache/incubator-hugegraph-toolchain/tree/master/hugegraph-hubble)/[tool](https://github.com/apache/incubator-hugegraph-toolchain/tree/master/hugegraph-tools)/[client](https://github.com/apache/incubator-hugegraph-toolchain/tree/master/hugegraph-client)**)
3. [hugegraph-ai](https://github.com/apache/incubator-hugegraph-ai) (integrated **Graph AI/LLM/KG** system)
4. [hugegraph-website](https://github.com/apache/hugegraph-doc) (**doc & website** code)


## Note

- If some classes under computer-k8s cannot be found, you need to execute `mvn clean install` in advance to generate corresponding classes.

## Contributing

- Welcome to contribute to HugeGraph, please see [How to Contribute](https://hugegraph.apache.org/docs/contribution-guidelines/contribute/) for more information.
- Note: It's recommended to use [GitHub Desktop](https://desktop.github.com/) to greatly simplify the PR and commit process.
- Thank you to all the people who already contributed to HugeGraph!

[![contributors graph](https://contrib.rocks/image?repo=apache/hugegraph-computer)](https://github.com/apache/incubator-hugegraph-computer/graphs/contributors)

## License

hugegraph-computer is licensed under [Apache 2.0](https://github.com/apache/incubator-hugegraph-computer/blob/master/LICENSE) License.

### Contact Us

---
 - [GitHub](https://github.com/apache/incubator-hugegraph-computer)
 - [GitHub Issues](https://github.com/apache/incubator-hugegraph-computer/issues): Feedback on usage issues and functional requirements (quick response)
 - Feedback Email: [dev@hugegraph.apache.org](mailto:dev@hugegraph.apache.org) ([subscriber](https://hugegraph.apache.org/docs/contribution-guidelines/subscribe/) only)
 - WeChat public account: Apache HugeGraph, welcome to scan this QR code to follow us.

 <img src="https://github.com/apache/hugegraph-doc/blob/master/assets/images/wechat.png?raw=true" alt="QR png" width="350"/>

