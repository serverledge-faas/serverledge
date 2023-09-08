#!/bin/bash

REVISION=$(ETCDCTL_API=3 etcdctl endpoint status --write-out="json" | egrep -o '"revision":[0-9]*' | egrep -o '[0-9].*')
ETCDCTL_API=3 etcdctl compact ${REVISION}
ETCDCTL_API=3 etcdctl defrag
etcdctl alarm disarm