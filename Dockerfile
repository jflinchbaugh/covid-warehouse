FROM debian:unstable
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -y \
    && apt-get upgrade -y \
    && apt-get install -y openjdk-17-jre-headless git time

RUN mkdir -p /app /data/in /data/out

WORKDIR /app

COPY target/uberjar/covid-warehouse-0.1.0-SNAPSHOT-standalone.jar app.jar
COPY entrypoint.sh entrypoint.sh

CMD ./entrypoint.sh