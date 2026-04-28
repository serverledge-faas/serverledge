#!/bin/sh
docker stop Etcd-server
docker rm Etcd-server
rm -rf default.etcd/