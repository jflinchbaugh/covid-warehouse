#!/bin/sh

cd /data/in

if [ -d .git ]; then
    git pull
else
    git clone https://github.com/CSSEGISandData/COVID-19.git .
fi

cd /app

java -server -XX:MaxRAMPercentage=80 -XX:MinRAMPercentage=80 \
     -jar app.jar \
     all \
     /data/in/csse_covid_19_data/csse_covid_19_daily_reports \
     /data/out
