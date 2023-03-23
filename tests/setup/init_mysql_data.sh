#! /usr/bin/env bash

# Exit in case of error
set -e

docker exec -i `docker-compose ps -q mysql | xargs docker inspect --format '{{ .Name }}' | sed 's/\/\(.*\)/\1/'` mysql -u root zillion_test < zillion_test.mysql.sql


