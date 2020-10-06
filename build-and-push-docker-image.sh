#!/bin/bash

docker build . -t erodriguezds/redis-rq:latest
docker push erodriguezds/redis-rq:latest
