# Vermeer Graph Compute Engine

## Introduction
Vermeer is a high-performance distributed graph computing platform based on memory, supporting more than 15 graph algorithms, custom algorithm extensions, and custom data source access.

## Start

```
master: ./vermeer --env=master
worker: ./vermeer --env=worker01
```
The parameter env specifies the name of the configuration file in the useconfig folder.

```
./vermeer.sh start master
./vermeer.sh start worker
```
Configuration items are specified in vermeer.sh
## supervisord
Can be used with supervisord to start and stop services, automatically start applications, rotate logs, and more; for the configuration file, refer to config/supervisor.conf;

Configuration file reference config/supervisor.conf

````
# run as daemon
./supervisord -c supervisor.conf -d
````

## Compile
Required
* go 1.23

### Install dependencies

```
go mod tidy
```

### Local compile

```
go build
```

---

### install grpc protobuf dependencies
````
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28.0 \
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0
````

### protobuf build
````
../../tools/protoc/osxm1/protoc *.proto --go-grpc_out=. --go_out=.
````


### Cross Compile

````
linux: GOARCH=amd64 GOOS=linux go build 
CC=x86_64-linux-musl-gcc CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -buildmode=plugin
````

---






