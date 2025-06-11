## Vermeer
Vermeer is a high-performance graph computing framework implemented in the Go language, mainly focusing on memory-based, with the latest version compatible with some data being stored on disk.

## Architecture Design
[Vermeer Architecture](docs/architecture.md)

## Build 

### grpc protobuf Dependency installation
````
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28.0 \
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0
````

### protobuf build

````
../../tools/protoc/osxm1/protoc *.proto --go-grpc_out=. --go_out=.
````

### Cross-compilation

````
linux: GOARCH=amd64 GOOS=linux go build 
CC=x86_64-linux-musl-gcc CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -buildmode=plugin
````

## Run

```
master: ./vermeer --env=master
worker: ./vermeer --env=worker01
# The parameter `env` specifies the configuration file name under the `config` folder.
```
OR

```
./vermeer.sh start master
./vermeer.sh start worker
# The configuration items are specified in the vermeer.sh file.
```


## API Doc
[Vermeer API](docs/api.md)


