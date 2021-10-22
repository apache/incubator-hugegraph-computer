#!/usr/bin/env bash

set -ev

TRAVIS_DIR=$(dirname "$0")

sh "${TRAVIS_DIR}"/start-etcd.sh
echo "Installing requirements..."
