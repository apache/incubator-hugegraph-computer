#!/bin/bash

set -ev

TRAVIS_DIR=`dirname $0`
echo "Starting etcd..."
chmod a+x ./computer-dist/src/main/resources/etcd
./computer-dist/src/main/resources/etcd &
echo "Installing requirments..."
