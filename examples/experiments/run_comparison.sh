#!/bin/bash

LB_IP="INSERIRE_IP_QUI"
export LB_POLICY=${1:-"RoundRobin"}

LOCUST_DURATION="20m"
USERS=9
SPAWN_RATE=9
RESULT_FILE="experiment_results.csv"

echo "============================================="
echo "STARTING EXPERIMENT"
echo "Policy: $LB_POLICY"
echo "Target: http://$LB_IP:1323"
echo "============================================="

locust -f locustfile.py \
    --headless \
    --users $USERS \
    --spawn-rate $SPAWN_RATE \
    --run-time $LOCUST_DURATION \
    --host "http://$LB_IP:1323"

echo "Experiment completed. Data appended to CSV."