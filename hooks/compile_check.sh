#!/usr/bin/env bash

make -j$(nproc)
if [ $? -ne 0 ]; then
    echo "编译失败，提交被拒绝" >&2
    exit 1
fi