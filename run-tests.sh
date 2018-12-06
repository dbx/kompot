#!/usr/bin/env sh

docker-compose -f docker-compose.yml up --abort-on-container-exit --build
