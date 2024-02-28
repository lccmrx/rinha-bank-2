#! /bin/bash

docker compose down
docker-compose up db -d

sleep 3
go run ./cmd > server.logs
