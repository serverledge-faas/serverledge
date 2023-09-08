#!/bin/bash
THIS_DIR=$(dirname "$0")
locust -f "${THIS_DIR}"/../../examples/experiments/experiment2/locust2.py -H http://192.168.1.14:1323