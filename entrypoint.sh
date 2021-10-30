#!/bin/sh

java -server -XX:MaxRAMPercentage=80 -XX:MinRAMPercentage=80 \
     -jar app.jar all /data/in /data/out
