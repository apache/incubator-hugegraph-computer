#! /bin/bash
export BUILD_REPO_WS=$PWD

go env -w GOPATH=$BUILD_REPO_WS/packages
go env -w GOCACHE=$BUILD_REPO_WS/cache
go env -w GO111MODULE="on"                      ## 开启 go mod 模式，必须

go env -w GONOSUMDB=\*                          ## 目前有一些代码库还不支持sumdb索引，暂时屏蔽此功能


#go env -w CC=/opt/compiler/gcc-8.2/bin/gcc
#go env -w CXX=/opt/compiler/gcc-8.2/bin/g++

go mod download
ARCH=$1
CGO_ENABLED=0 GOOS=linux GOARCH="$ARCH" go build

VERSION=$(cat ./apps/version/version.go | grep 'Version' | awk -F '"' '{print $2}')
cp tools/supervisord/linux_"$ARCH"/supervisord supervisord
tar --exclude=config/afs_client.conf -zcvf vermeer-"$VERSION"-"$ARCH".tar.gz vermeer config/ supervisord vermeer.sh mem_supervisor.sh

mkdir "$BUILD_REPO_WS"/output
mv vermeer-"$VERSION"-"$ARCH".tar.gz "$BUILD_REPO_WS"/output/
