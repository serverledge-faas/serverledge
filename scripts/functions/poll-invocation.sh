#!/bin/sh

THIS_DIR=$(dirname "$0")
echo $1
"$THIS_DIR"/../../bin/serverledge-cli poll --request $1