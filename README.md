# covid-warehouse

Data warehouse built with pure Clojure to analyze COVID-19 data from JHU.

## Run the Warehouse from Source
`lein run all ../COVID-19/csse_covid_19_data/csse_covid_19_daily_reports output`

## Run the Warehouse from Uberjar
```
java -server -XX:MaxRAMPercentage=80 -XX:MinRAMPercentage=80 \
     -jar app.jar all \
     ../COVID-19/csse_covid_19_data/csse_covid_19_daily_reports \
     output
```

## Build the Warehouse into a Container (with Podman)
`make`

## Run the Warehouse from a Container (with Podman)
```
podman run -m 8g --rm -it \
       -v ./output:/data/out \
       -v covid-data:/data/in \
       localhost/covid-warehouse
```
