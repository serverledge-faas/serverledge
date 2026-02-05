#!/bin/sh
docker run --rm -d -p 9091:9091 --name pushgateway prom/pushgateway
