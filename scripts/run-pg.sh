#!/bin/bash

docker build -t postgres -f ../Dockerfile.postgres .
docker run -p 5432:5432 -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=testdb postgres
