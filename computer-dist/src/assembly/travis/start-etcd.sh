#!/bin/bash

set -ev

TRAVIS_DIR=`dirname $0`
echo "Starting etcd..."
chmod a+x ./computer-dist/src/main/resources/etcd
./computer-dist/src/main/resources/etcd --name etcd-test \
--initial-advertise-peer-urls http://localhost:2580 \
--listen-peer-urls http://localhost:2580 \
--advertise-client-urls http://localhost:2579 \
--listen-client-urls http://localhost:2579 &
