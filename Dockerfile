FROM debian:unstable
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -y \
    && apt-get upgrade -y \
    && apt-get install -y openjdk-16-jre-headless

RUN mkdir -p /app /data/in /data/out

WORKDIR /app

COPY target/uberjar/covid-warehouse-0.1.0-SNAPSHOT-standalone.jar app.jar
COPY resources/web web

RUN chown -R 1000 /app /data/in /data/out

USER 1000

CMD java -server -Xmx2500m -Xms2500m -jar app.jar all /data/in /data/out