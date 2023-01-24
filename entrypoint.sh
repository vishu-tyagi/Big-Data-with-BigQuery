#!/bin/bash

case $1 in
    extract-load)
    callable="extract-load"
    ;;
    *)
    echo "Invalid endpoint $1"
    exit 1
    ;;
esac

CMD="python3 -m nyc_taxi $callable ${@:2}"

exec $CMD