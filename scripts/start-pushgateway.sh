#!/bin/sh
docker run -d -p 9091:9091 --name pushgateway prom/pushgateway
