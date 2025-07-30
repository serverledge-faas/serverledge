#!/bin/sh

THIS_DIR=$(dirname "$0")

"$THIS_DIR"/../../bin/serverledge-cli delete-workflow -f sequence
