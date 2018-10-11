#!/usr/bin/env bash

# Install ETCD
VERSION=2.3.7
URL="https://github.com/coreos/etcd/releases/download/v${VERSION}/etcd-v${VERSION}-linux-amd64.tar.gz"
curl -L $URL | tar -C . --strip-components=1 -xzvf - "etcd-v${VERSION}-linux-amd64/etcd"

# Start EtcD
chmod u+x etcd
./etcd --listen-client-urls http://0.0.0.0:12379 --advertise-client-urls http://0.0.0.0:12379 &

# Test
npm test

# Stop EtcD
kill %1
