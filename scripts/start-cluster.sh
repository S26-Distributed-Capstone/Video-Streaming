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

echo ">>> Starting Control Plane Nodes..."
for ip in "${CONTROL_PLANES[@]}"; do
  echo "Connecting to ${ip}..."
  ssh -t "sack@${ip}" "doas rc-service k3s start"
done

echo
echo ">>> Starting Worker Nodes..."
for ip in "${WORKER_NODES[@]}"; do
  echo "Connecting to ${ip}..."
  # Try k3s-agent first; if that service does not exist, fall back to k3s.
  ssh -t "sack@${ip}" "doas rc-service k3s-agent start || doas rc-service k3s start"
done

echo
echo ">>> All start commands sent. Waiting 10 seconds for sockets to initialize..."
sleep 10
