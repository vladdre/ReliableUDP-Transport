#!/bin/bash

if [ ! -f README ]; then
    echo "No README present! Exiting..." >&2
    exit 1
fi

if [ ! -f Makefile ]; then
    echo "No Makefile present! Exiting..." >&2
    exit 1
fi

rm -rf archive.zip
make clean
zip -r archive.zip lib/ *.c Makefile README
