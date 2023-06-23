#!/bin/bash

export MALLOC_ARENA_MAX=2
#export JAVA_OPTS="-server -XX:MaxRAMPercentage=75 -XX:MinRAMPercentage=75 --add-opens java.base/java.util.concurrent=ALL-UNNAMED"
export JAVA_OPTS="-server --add-opens java.base/java.util.concurrent=ALL-UNNAMED"
lein run all ../COVID-19/csse_covid_19_data/csse_covid_19_daily_reports output
