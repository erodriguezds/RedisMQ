#!/bin/bash

redis-server --loadmodule bin/redisrq.so redis.conf &

