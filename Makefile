SHELL := /bin/bash
CWD := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

BASE_IMG = nyc-taxi-base
IMG = nyc-taxi
IMG_TAG ?= latest

LOCAL_DATA_DIR = ${CWD}data
DOCKER_DATA_DIR = /usr/src/app/data

export

.PHONY: build-base
build-base:
	@docker build -t ${BASE_IMG}:${IMG_TAG} -f Dockerfile.base .

