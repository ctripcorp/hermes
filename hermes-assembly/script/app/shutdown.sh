#!/bin/bash

# Define script absolute path.
SCRIPT_PATH=$(readlink -f $0)

# Define script base dir.
SCRIPT_DIR=$(dirname $SCRIPT_PATH)

chmod +x $SCRIPT_DIR/__startup.sh
$SCRIPT_DIR/__startup.sh stop
