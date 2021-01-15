#!/bin/bash

set -ev

TRAVIS_DIR=`dirname $0`

sh start-etcd.sh
echo "Installing requirments..."
