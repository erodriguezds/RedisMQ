#!/bin/bash

rm dump.rdb
make clean && make
redis-cli module load bin/module.so
