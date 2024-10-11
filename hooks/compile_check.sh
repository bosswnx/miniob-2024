#!/usr/bin/env bash

cd build || (echo "please build binary in 'build' directory" && exit 1)

cmake --build .
if [ $? -ne 0 ]; then
    echo "compile failed, commit refused" >&2
    exit 1
fi