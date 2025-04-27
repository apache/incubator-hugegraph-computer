# Vermeer图计算平台

## 简介
Vermeer是一个基于内存的高性能分布式图计算平台，支持15+图算法。支持自定义算法扩展，支持自定义数据源接入。

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

## 编译

* Go 1.23

### 安装依赖项

```
go mod tidy
```

### 本地编译

```
go build
```

### grpc protobuf 依赖项安装
````
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28.0 \
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0
````



### protobuf build
生成protobuf文件
````
../../tools/protoc/osxm1/protoc *.proto --go-grpc_out=. --go_out=.
````



### 交叉编译

````
linux: GOARCH=amd64 GOOS=linux go build 
CC=x86_64-linux-musl-gcc CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -buildmode=plugin
````

---



# 使用hubble平台
有三种搭建方式，参考https://hugegraph.apache.org/docs/quickstart/hugegraph-hubble/

Use Docker (Convenient for Test/Dev)
Download the Toolchain binary package
Source code compilation
