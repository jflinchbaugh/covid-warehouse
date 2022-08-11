# covid-warehouse

Data warehouse built with pure Clojure to analyze COVID-19 data from JHU.

## Run the Warehouse from Source
```
export MALLOC_ARENA_MAX=2
export JAVA_OPTS="-server -XX:MaxRAMPercentage=75 -XX:MinRAMPercentage=75 --add-opens java.base/java.util.concurrent=ALL-UNNAMED"
lein run all ../COVID-19/csse_covid_19_data/csse_covid_19_daily_reports output
```

## Run the Warehouse from Uberjar

```
export MALLOC_ARENA_MAX=2
java -server -XX:MaxRAMPercentage=75 -XX:MinRAMPercentage=75 \
     --add-opens java.base/java.util.concurrent=ALL-UNNAMED \
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

## Start as a Pod in Podman

The `covid-warehouse.yaml` describes a Pod with the warehouse.
The `./run_pod.sh` script will rebuild the covid-warehouse image and launch the pod. 
When the warehouse process finishes, the script will copy the results out
of the volume into the `./output/` directory and shutdown the whole pod.
