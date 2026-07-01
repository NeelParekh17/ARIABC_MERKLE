#!/usr/bin/env bash

# cluster_topology.sh
# Common Cluster Topology for AriaBC Distributed Benchmarks and Tests

declare -a NODE_IDS=(1 2 4)
declare -a NODE_IPS=(10.129.148.236 10.129.148.246 10.129.148.248)
declare -a NODE_NAMES=(admin123 user4 utkarsh)
declare -a NODE_USERS=(neel neel neel)

declare -a NODE_IS_U22=(0 1 0)
declare -a NODE_CLIENT_PORTS=(8000 8000 8001)

export RAFT_PORT=9000
export DB_PORT=5438
export DB_USER=postgres
export DB_NAME=postgres
