#!/usr/bin/env bash

set -euo pipefail

CONTROL_PLANES=(
  "192.168.8.11"
  "192.168.8.12"
  "192.168.8.13"
)

WORKER_NODES=(
  #"192.168.8.101"
  #"192.168.8.102"
  #"192.168.8.103"
  #"192.168.8.104"
  #"192.168.8.105"
  #"192.168.8.106"
  #"192.168.8.108"
  "192.168.8.109"
  "192.168.8.110"
  "192.168.8.111"
  "192.168.8.112"
)

NODES=("${CONTROL_PLANES[@]}" "${WORKER_NODES[@]}")
PUB_KEY_PATH="${HOME}/.ssh/id_rsa.pub"

if [[ ! -f "${PUB_KEY_PATH}" ]]; then
  echo "ERROR: ${PUB_KEY_PATH} not found. Run 'ssh-keygen -t rsa -b 4096' first." >&2
  exit 1
fi

PUB_KEY="$(<"${PUB_KEY_PATH}")"

for ip in "${NODES[@]}"; do
  echo
  echo ">>> Authorizing key on ${ip}..."

  if ssh -o StrictHostKeyChecking=accept-new "sack@${ip}" \
    "mkdir -p ~/.ssh && chmod 700 ~/.ssh && touch ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys && if ! grep -qxF '${PUB_KEY}' ~/.ssh/authorized_keys; then echo '${PUB_KEY}' >> ~/.ssh/authorized_keys; fi"; then
    echo "OK: ${ip}"
  else
    echo "Failed to authorize key on ${ip}" >&2
  fi
done

echo
echo ">>> Verifying passwordless SSH..."
for ip in "${NODES[@]}"; do
  if ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new "sack@${ip}" "hostname" >/dev/null; then
    echo "OK: ${ip}"
  else
    echo "Verification FAILED on ${ip}" >&2
  fi
done

echo
echo ">>> Key distribution complete."
