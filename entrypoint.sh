#!/bin/bash

#Set DEBUG=true to debug container
DEBUG=false
if ${DEBUG}; then
    if [ -f "already_ran" ]; then
        echo "Already ran the Entrypoint once. Holding indefinitely for debugging."
        cat
    fi
    touch already_ran
fi

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