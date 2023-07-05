#!/bin/bash

export MALLOC_ARENA_MAX=2
export JAVA_OPTS="-server -XX:MaxRAMPercentage=50 -XX:MinRAMPercentage=50 --add-opens java.base/java.util.concurrent=ALL-UNNAMED"
#export JAVA_OPTS="-server --add-opens java.base/java.util.concurrent=ALL-UNNAMED"
lein run all input output
