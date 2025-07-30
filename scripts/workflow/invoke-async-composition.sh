#!/bin/sh

THIS_DIR=$(dirname "$0")

"$THIS_DIR"/../../bin/serverledge-cli invoke-workflow -f sequence -p "input:1" --async
