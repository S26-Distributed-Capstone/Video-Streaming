#!/bin/sh
set -e
set -a
. ./.env
set +a


STACK_NAME="video"
IMAGE_NAME="video-streaming-app:latest"
STACK_FILE="docker_compose.swarm.yaml"

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
if ! docker network ls --format '{{.Name}}' | grep -q "^${STACK_NAME}_default$"; then
  docker network create --driver overlay "${STACK_NAME}_default" >/dev/null
fi

echo "Deploying stack ${STACK_NAME}..."
docker stack deploy -c "${STACK_FILE}" "${STACK_NAME}"

echo "Scaling streaming-service to 3 replicas..."
docker service scale "${STACK_NAME}_streaming-service=3"

echo "Scaling upload-service to 3 replicas..."
docker service scale "${STACK_NAME}_upload-service=3"

echo "Scaling processing-service to 3 replicas..."
docker service scale "${STACK_NAME}_processing-service=3"

echo "Scaling node-watcher to 3 replicas..."
docker service scale "${STACK_NAME}_node-watcher=3"

echo "Done."
