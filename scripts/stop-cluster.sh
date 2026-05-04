#!/usr/bin/env bash

set -euo pipefail

CONTROL_PLANES=(
  "192.168.8.11"
  "192.168.8.12"
  "192.168.8.13"
)

WORKER_NODES=(
  "192.168.8.101"
  "192.168.8.102"
  "192.168.8.103"
  "192.168.8.104"
  "192.168.8.105"
  "192.168.8.106"
  "192.168.8.108"
  "192.168.8.109"
  "192.168.8.110"
  "192.168.8.111"
  "192.168.8.112"
)

echo ">>> Stopping Worker Nodes..."
for ip in "${WORKER_NODES[@]}"; do
  echo "Connecting to ${ip}..."
  # Try k3s-agent first, fall back to k3s.
  ssh "sack@${ip}" "doas rc-service k3s-agent stop || doas rc-service k3s stop"
done

echo
echo ">>> Stopping Control Plane Nodes..."
for ip in "${CONTROL_PLANES[@]}"; do
  echo "Connecting to ${ip}..."
  ssh "sack@${ip}" "doas rc-service k3s stop"
done

echo
echo ">>> All stop commands sent. Cluster is shutting down."
