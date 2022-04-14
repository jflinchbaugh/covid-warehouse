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

## The Warehouse in a Container (with Podman)

The container clones or updates a COVID-19 data set from JHU into a volume,
and starts up the ETL process using a large portion of the memory allocated
to the container.

To build it: `make`

## Run the Warehouse from a Container (with Podman)
```
podman run -m 8g --rm -it \
       -v ./output:/data/out \
       -v covid-data:/data/in \
       localhost/covid-warehouse
```

## Start as a Pod with Postgres in Podman

The `covid-warehouse.yaml` describes a Pod with the warehouse and a PostgreSQL
server deployed together.

`./run_pod.sh`
