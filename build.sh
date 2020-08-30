#!/bin/bash
gcc -fPIC -std=gnu99 -c -o bin/module.o src/module.c
ld -o bin/module.so bin/module.o -shared -Bsymbolic -lc