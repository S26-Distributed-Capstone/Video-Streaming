# RabbitMQ Setup Guide

## Starting RabbitMQ

```bash
docker-compose -f rabbitmq_docker_compose.yaml up -d
```

## Accessing the Management UI

- URL: http://localhost:15672
- Username: `admin`
- Password: `admin123`

## Stopping RabbitMQ

```bash
docker-compose -f rabbitmq_docker_compose.yaml down
```

## Verifying RabbitMQ is Running

```bash
docker ps | grep rabbitmq
```

Check health status (should show `healthy`):

```bash
docker inspect video-streaming-rabbitmq | grep -A 5 Health
```

## What RabbitMQ Does in This System

RabbitMQ serves as the message broker for asynchronous inter-service communication:

- **Upload service** publishes events when videos are uploaded
- **Processing service** subscribes to upload events to trigger transcoding
- **Upload service** receives status updates from the processing service and pushes them to connected clients via WebSocket

This decouples the services — no service calls another directly, instead communicating through RabbitMQ.

## Stopping and Removing Data

To stop the container and remove the volume (full reset):

```bash
docker-compose -f rabbitmq_docker_compose.yaml down -v
```

To stop without removing data:

```bash
docker-compose -f rabbitmq_docker_compose.yaml down
```

## Troubleshooting

### Container won't start
- Check if ports `5672` or `15672` are already in use:
  ```bash
  # Windows
  netstat -ano | findstr :5672
  netstat -ano | findstr :15672
  # Linux
  ss -ltnp | grep 5672
  ```
- Check Docker logs:
  ```bash
  docker logs video-streaming-rabbitmq
  ```

### Can't access management UI
- Verify container is running: `docker ps`
- Check health status: `docker inspect video-streaming-rabbitmq`

### Health check failing
- Wait a few seconds after startup — RabbitMQ can take a moment to become ready
- Re-check with: `docker ps` and look at the `STATUS` column for `(healthy)`

## Notes
- Default credentials should be changed in production
- The management UI is useful for debugging message flow during development
- Volume `rabbitmq_data` persists data between container restarts
- This setup only provides the infrastructure — actual queue/exchange definitions are handled by the pub/sub abstraction layer
