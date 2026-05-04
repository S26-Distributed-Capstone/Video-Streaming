#!/usr/bin/env bash

set -euo pipefail

NODES=(
  "192.168.8.11"
  "192.168.8.12"
  "192.168.8.13"
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

SOURCE_FILE="dist/images-amd64.tar"
DESTINATION_PATH="/home/sack/videostreaming/images-amd64.tar"

for node in "${NODES[@]}"; do
  echo
  echo ">>> Processing node: ${node}"

  ssh "sack@${node}" "mkdir -p /home/sack/videostreaming"

  echo "Transferring ${SOURCE_FILE}..."
  if scp "${SOURCE_FILE}" "sack@${node}:${DESTINATION_PATH}"; then
    echo "Importing images on ${node}..."
    ssh -t "sack@${node}" "doas k3s ctr -n k8s.io images import ${DESTINATION_PATH}"
  else
    echo "Failed to transfer to ${node}. Skipping import." >&2
  fi
done

echo
echo ">>> Distribution and Import Complete!"
