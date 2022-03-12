#!/bin/sh

podman play kube covid-warehouse.yaml

podman pod ls
podman ps

podman logs -f covid-warehouse-etl

podman pod rm -f covid-warehouse

# dump output volume locally 
volume=in
localdir=output

mkdir -p $localdir

id=$(podman create -v $volume:/data busybox)
podman cp $id:/data $localdir
podman rm -v $id > /dev/null
