SHELL := /bin/bash
CWD := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

BASE_IMG = nyc-taxi-base
IMG = nyc-taxi
IMG_TAG ?= latest

LOCAL_DATA_DIR = ${CWD}data
DOCKER_DATA_DIR = /usr/src/app/data

export

.PHONY: check-connection
check-connection:
	@[ "${network}" ] || ( echo ">> network is not set"; exit 1 )
	@[ "${user}" ] || ( echo ">> user is not set"; exit 1 )
	@[ "${password}" ] || ( echo ">> password is not set"; exit 1 )
	@[ "${host}" ] || ( echo ">> host is not set"; exit 1 )
	@[ "${port}" ] || ( echo ">> port is not set"; exit 1 )
	@[ "${db}" ] || ( echo ">> db is not set"; exit 1 )
	@[ "${schema}" ] || ( echo ">> schema is not set"; exit 1 )

.PHONY: build-base
build-base:
	@docker build \
		-t ${BASE_IMG}:${IMG_TAG} \
		-f Dockerfile.base .

.PHONY: build
build: build-base
	docker build \
		-t ${IMG}:${IMG_TAG} \
		--build-arg IMAGE=${BASE_IMG}:${IMG_TAG} .

extract-load: check-connection
	docker run -t \
		--network=$(network) \
		-v ${LOCAL_DATA_DIR}:${DOCKER_DATA_DIR} \
		nyc-taxi:latest extract-load \
		--user=$(user) \
		--password=$(password) \
		--host=$(host) \
		--port=$(port) \
		--db=$(db) \
		--schema=$(schema)
