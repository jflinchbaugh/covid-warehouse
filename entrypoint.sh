#!/bin/sh

cd /data/in

if [ -d .git ]; then
    git pull --depth=1
else
    git clone --depth=1 https://github.com/CSSEGISandData/COVID-19.git .
fi

cd /app

export MALLOC_ARENA_MAX=2

time -p java \
    -server \
    -XX:MaxRAMPercentage=70 -XX:MinRAMPercentage=70 \
    --add-opens java.base/java.util.concurrent=ALL-UNNAMED \
    -jar app.jar \
    all \
    /data/in/csse_covid_19_data/csse_covid_19_daily_reports \
    /data/out
