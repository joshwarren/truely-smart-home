#!/usr/bin/env bash

# Create postgres database and network backend
# docker network create postgres_backend

docker volume create \
  --opt device="C:\Users\jwarren\Documents\Sarum\data" \
  --opt o=bind \
  --opt type=none \
  postgres_data

docker run --name postgres \
  -e POSTGRES_PASSWORD=test \
  -e POSTGRES_USER=test \
  -d --restart always \
  -v postgres_data:/var/lib/postgresql/data \
  --net postgres_backend \
  postgres:13
  # -v "C:\Users\jwarren\Documents\Sarum\data":/var/lib/postgresql/data \
#/var/lib/postgresql/data
# docker build -t sarum .
# docker run --net postgres_backend --detach sarum