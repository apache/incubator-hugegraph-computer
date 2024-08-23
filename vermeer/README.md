# 图计算平台

### grpc protobuf 依赖项安装

````
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28.0 \
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0
````

---

### protobuf build

````
../../tools/protoc/osxm1/protoc *.proto --go-grpc_out=. --go_out=.
````

---

### 交叉编译

````
linux: GOARCH=amd64 GOOS=linux go build 
CC=x86_64-linux-musl-gcc CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -buildmode=plugin
````

---

### 运行

```
master: ./vermeer --env=master
worker: ./vermeer --env=worker01
# 参数env是指定使用config文件夹下的配置文件名
or
./vermeer.sh start master
./vermeer.sh start worker
# 配置项在vermeer.sh中指定
```

---

## supervisord

配置文件参考 config/supervisor.conf

````
# 启动 run as daemon
./supervisord -c supervisor.conf -d
````
