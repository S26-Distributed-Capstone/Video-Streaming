#!/usr/bin/env bash

set -euo pipefail

CONTROL_PLANES=(
  "192.168.8.11"
  "192.168.8.12"
  "192.168.8.13"
)

WORKER_NODES=(


  "192.168.8.109"
  "192.168.8.110"
  "192.168.8.111"
  "192.168.8.112"
)

NODES=("${CONTROL_PLANES[@]}" "${WORKER_NODES[@]}")
DOAS_RULE="permit nopass sack"

for ip in "${NODES[@]}"; do
  echo
  echo ">>> Configuring doas on ${ip}..."

  if ssh -t "sack@${ip}" \
    "doas sh -c 'mkdir -p /etc/doas.d && touch /etc/doas.d/doas.conf && if ! grep -qxF \"${DOAS_RULE}\" /etc/doas.d/doas.conf; then printf \"%s\n\" \"${DOAS_RULE}\" >> /etc/doas.d/doas.conf; fi && chmod 600 /etc/doas.d/doas.conf'"; then
    echo "OK: ${ip}"
  else
    echo "Failed to configure doas on ${ip}" >&2
  fi
done

echo
echo ">>> Verifying passwordless doas..."
for ip in "${NODES[@]}"; do
  if ssh -o BatchMode=yes "sack@${ip}" "doas -n true" >/dev/null; then
    echo "OK: ${ip}"
  else
    echo "Verification FAILED on ${ip} (doas still requires a password)" >&2
  fi
done

echo
echo ">>> doas configuration complete."
