all: container

.PHONY: uberjar

uberjar:
	lein clean
	lein test
	lein uberjar

.PHONY: container

container: uberjar
	podman build -t covid-warehouse:latest .

