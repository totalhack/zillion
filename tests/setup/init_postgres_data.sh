#! /usr/bin/env bash

# Exit in case of error
set -e

docker exec -i -u postgres `docker-compose ps -q postgres | xargs docker inspect --format '{{ .Name }}' | sed 's/\/\(.*\)/\1/'` psql -U postgres -d zillion_test < zillion_test.postgres.sql
