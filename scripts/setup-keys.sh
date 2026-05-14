#!/usr/bin/env bash

set -euo pipefail

SSH_KEY="${SSH_KEY:-${HOME}/.ssh/cluster_key}"
SSH_OPTS=(-o StrictHostKeyChecking=accept-new)

if [[ -f "$SSH_KEY" ]]; then
  SSH_OPTS=(-i "$SSH_KEY" "${SSH_OPTS[@]}")
fi

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
  #"192.168.8.108"
  "192.168.8.109"
  "192.168.8.110"
  "192.168.8.111"
  "192.168.8.112"
)

NODES=("${CONTROL_PLANES[@]}" "${WORKER_NODES[@]}")

resolve_pub_key_path() {
  local candidate
  for candidate in \
    "${PUB_KEY_PATH:-}" \
    "${SSH_KEY}.pub" \
    "${HOME}/.ssh/id_ed25519.pub" \
    "${HOME}/.ssh/id_rsa.pub"; do
    [[ -n "$candidate" && -f "$candidate" ]] && {
      printf '%s\n' "$candidate"
      return 0
    }
  done
  return 1
}

PUB_KEY_PATH="$(resolve_pub_key_path || true)"

if [[ -z "${PUB_KEY_PATH}" ]]; then
  echo "ERROR: no public SSH key found." >&2
  echo "Looked for: ${SSH_KEY}.pub, ~/.ssh/id_ed25519.pub, ~/.ssh/id_rsa.pub" >&2
  exit 1
fi

PUB_KEY="$(<"${PUB_KEY_PATH}")"

for ip in "${NODES[@]}"; do
  echo
  echo ">>> Authorizing key on ${ip}..."

  if ssh "${SSH_OPTS[@]}" "sack@${ip}" \
    "mkdir -p ~/.ssh && chmod 700 ~/.ssh && touch ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys && if ! grep -qxF '${PUB_KEY}' ~/.ssh/authorized_keys; then echo '${PUB_KEY}' >> ~/.ssh/authorized_keys; fi"; then
    echo "OK: ${ip}"
  else
    echo "Failed to authorize key on ${ip}" >&2
  fi
done

echo
echo ">>> Verifying passwordless SSH..."
for ip in "${NODES[@]}"; do
  if ssh "${SSH_OPTS[@]}" -o BatchMode=yes "sack@${ip}" "hostname" >/dev/null; then
    echo "OK: ${ip}"
  else
    echo "Verification FAILED on ${ip}" >&2
  fi
done

echo
echo ">>> Key distribution complete."
