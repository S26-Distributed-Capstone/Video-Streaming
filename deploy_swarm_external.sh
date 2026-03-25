#!/bin/sh
set -e
set -a
if [ -f ./.env ]; then
  . ./.env
elif [ -f ./.env.example ]; then
  echo "Warning: .env not found, falling back to .env.example" >&2
  . ./.env.example
else
  echo "Error: neither .env nor .env.example exists. Please create a .env file before running this script." >&2
  exit 1
fi
set +a

STACK_NAME="video-external"
IMAGE_NAME="video-streaming-app:latest"
STACK_FILE="docker_compose.swarm.external.yaml"

echo "Building image ${IMAGE_NAME}..."
docker build -t "${IMAGE_NAME}" .

echo "Removing existing stack (if any)..."
docker stack rm "${STACK_NAME}" || true
sleep 2

echo "Waiting for stack to be removed..."
for i in $(seq 1 30); do
  if ! docker stack ls --format '{{.Name}}' | grep -q "^${STACK_NAME}$"; then
    break
  fi
  sleep 1
done

echo "Ensuring overlay network exists..."
if ! docker network ls --format '{{.Name}}' | grep -q "^video_default$"; then
  docker network create --driver overlay video_default >/dev/null
fi

echo "Deploying stack ${STACK_NAME}..."
docker stack deploy -c "${STACK_FILE}" "${STACK_NAME}"

echo "Done."
