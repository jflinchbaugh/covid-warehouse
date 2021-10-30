#!/bin/sh

java -server -XX:MaxRAMPercentage=50 -XX:MinRAMPercentage=50 \
     -jar app.jar all /data/in /data/out
