# Vermeer图计算平台

## 简介
Vermeer是一个基于内存的高性能分布式图计算平台，支持15+图算法。支持自定义算法扩展，支持自定义数据源接入。

## 基于 Docker 运行

拉取镜像
```
docker pull hugegraph/vermeer:latest
```

创建好本地配置文件，例如`~/master.ini`与`~/worker.ini`

基于docker运行，其中`--env`指定的是文件名称。
```
master: docker run -v ~/:/go/bin/config hugegraph/vermeer --env=master
worker: docker run -v ~/:/go/bin/config hugegraph/vermeer --env=worker
```

我们也提供了`docker-compose`文件，当创建好`~/master.ini`与`~/worker.ini`，将`worker.ini`中的`master_peer`修改为`172.20.0.10:6689`后，即可通过以下命令运行：
```
docker-compose up -d
```

## 运行

```
master: ./vermeer --env=master
worker: ./vermeer --env=worker01
```
参数env是指定使用config文件夹下的配置文件名

```
./vermeer.sh start master
./vermeer.sh start worker
```
配置项在vermeer.sh中指定


## supervisord
可搭配supervisord使用，启动和停止服务，应用自动拉起、日志轮转等功能；

配置文件参考 config/supervisor.conf

````
# 启动 run as daemon
./supervisord -c supervisor.conf -d
````

## 从源码编译

### 环境要求
* Go 1.23 或更高版本
* `curl` 和 `unzip` 工具(用于下载依赖)
* 互联网连接(首次构建时需要)

### 快速开始

**推荐**: 使用 Makefile 进行构建:

```bash
# 首次设置(下载二进制依赖)
make init

# 构建 vermeer
make
```

**替代方案**: 使用构建脚本:

```bash
# AMD64 架构
./build.sh amd64

# ARM64 架构
./build.sh arm64
```

构建过程会自动:
1. 下载所需的二进制工具(supervisord, protoc)
2. 生成 Web UI 资源文件
3. 构建 vermeer 二进制文件

### 构建目标

```bash
make build              # 为当前平台构建
make build-linux-amd64  # 为 Linux AMD64 构建
make build-linux-arm64  # 为 Linux ARM64 构建
make clean              # 清理生成的文件
make help               # 显示所有可用目标
```

### 开发构建

如需在开发环境中热重载 Web UI:

```bash
go build -tags=dev
```

---

### Protobuf 开发

如需重新生成 protobuf 文件:

```bash
# 安装 protobuf Go 插件
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28.0
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0

# 生成 protobuf 文件
tools/protoc/osxm1/protoc *.proto --go-grpc_out=. --go_out=.
```

---



# 使用hubble平台
有三种搭建方式，参考https://hugegraph.apache.org/docs/quickstart/hugegraph-hubble/

Use Docker (Convenient for Test/Dev)
Download the Toolchain binary package
Source code compilation
