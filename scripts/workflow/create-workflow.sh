#!/bin/sh

THIS_DIR=$(dirname "$0")

"$THIS_DIR"/../../bin/serverledge-cli create-workflow -f sequence -s ./../../examples/workflow-simple.json 
