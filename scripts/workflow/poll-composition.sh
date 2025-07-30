#!/bin/sh

THIS_DIR=$(dirname "$0")

"$THIS_DIR"/../../bin/serverledge-cli poll --request "$1"