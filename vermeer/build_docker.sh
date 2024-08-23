#!/usr/bin/env bash
VERSION=$(cat ./apps/version/version.go | grep 'Version' | awk -F '"' '{print $2}')


USERNAME=$1 # 用户邮箱前缀
PASSWORD=$2 # 镜像仓库控制台个人中心设置的密码，不是UUAP密码
if [ -z ${USERNAME} ]; then
    echo "Enter Your Name: "
    read USERNAME
fi
if [ -z ${PASSWORD} ]; then
    echo "Enter Password: "
    read -s PASSWORD
fi

NS=hugegraph-vermeer # 空间名称
IMAGE=vermeer #仓库名称
TAG=${VERSION} # 镜像的tag
