SHELL := /bin/bash
CWD := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

BASE_IMG = nyc-taxi-base
IMG = nyc-taxi
IMG_TAG ?= latest

LOCAL_DATA_DIR = ${CWD}data
DOCKER_DATA_DIR = /usr/src/app/data

export

.PHONY: check-env
check-env:
ifndef GOOGLE_APPLICATION_CREDENTIALS
	$(error GOOGLE_APPLICATION_CREDENTIALS is undefined)
endif
ifndef GCP_PROJECT_ID
	$(error GCP_PROJECT_ID is undefined)
endif
ifndef GCP_GCS_BUCKET
	$(error GCP_GCS_BUCKET is undefined)
endif

.PHONY: check-requirments
check-requirments:
	@[ "${taxi}" ] || ( echo ">> taxi is not set"; exit 1 )
	@[ "${year}" ] || ( echo ">> year is not set"; exit 1 )
	@[ "${month}" ] || ( echo ">> month is not set"; exit 1 )

.PHONY: build-base
build-base:
	@docker build \
		-t ${BASE_IMG}:${IMG_TAG} \
		-f Dockerfile.base .

.PHONY: build
build: build-base
	@docker build \
		-t ${IMG}:${IMG_TAG} \
		--build-arg IMAGE=${BASE_IMG}:${IMG_TAG} .

extract-load: check-env check-requirments
	@docker run -t \
		-v ${GOOGLE_APPLICATION_CREDENTIALS}:${GOOGLE_APPLICATION_CREDENTIALS}:ro \
		-v ${LOCAL_DATA_DIR}:${DOCKER_DATA_DIR} \
		-e GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS} \
		-e GCP_PROJECT_ID=${GCP_PROJECT_ID} \
		-e GCP_GCS_BUCKET=${GCP_GCS_BUCKET} \
		nyc-taxi:latest extract-load \
		--taxi=$(taxi) \
		--year=$(year) \
		--month=$(month)
