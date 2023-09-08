#!/bin/sh

THIS_DIR=$(dirname "$0")

"$THIS_DIR"/../../bin/serverledge-cli create -f inc --memory 40 --src "$THIS_DIR"/../../examples/inc.py --runtime python310 --handler "inc.handler"