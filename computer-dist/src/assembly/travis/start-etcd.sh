#!/usr/bin/env bash

set -ev

TRAVIS_DIR=$(dirname "$0")
echo "Starting etcd..."
wget -O "${TRAVIS_DIR}"/etcd.tar.gz https://github.com/etcd-io/etcd/releases/download/v3.5.0/etcd-v3.5.0-linux-amd64.tar.gz
mkdir "${TRAVIS_DIR}"/etcd
tar -zxvf "${TRAVIS_DIR}"/etcd.tar.gz -C "${TRAVIS_DIR}"/etcd --strip-components 1
chmod a+x "${TRAVIS_DIR}"/etcd
"${TRAVIS_DIR}"/etcd/etcd --name etcd-test \
    --initial-advertise-peer-urls http://localhost:2580 \
    --listen-peer-urls http://localhost:2580 \
    --advertise-client-urls http://localhost:2579 \
    --listen-client-urls http://localhost:2579 &
