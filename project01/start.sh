#!/bin/bash

set -e

echo "Starting RabbitMQ..."

rabbitmq-server -detached

echo "Waiting for RabbitMQ..."

until rabbitmq-diagnostics -q ping; do
    sleep 1
done

echo "RabbitMQ is ready"

echo "Building programs..."

go build -o shared/shared ./shared
go build -o client/client ./client
go build -o manager/manager ./manager
go build -o worker/worker ./worker
go build -o etl/etl ./etl

echo "Starting client..."

./client/client "$@"
