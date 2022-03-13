#!/bin/bash

function cleanup() {
    echo "Shutting down..."
    podman pod rm -f covid-warehouse
    exit
}

make

podman play kube covid-warehouse.yaml

trap cleanup EXIT

podman pod ls
podman ps

podman logs -f covid-warehouse-etl

# dump output volume locally 
volume=out
localdir=output

mkdir -p $localdir

id=$(podman create -v $volume:/data busybox)
podman cp $id:/data $localdir
podman rm -v $id > /dev/null
