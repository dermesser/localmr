#!/bin/bash

# This script generates 11 files with random alphanumeric strings.
# Those are used by certain tests to test and measure the performance of
# sharded sort implementations.

for I in {0..10}; do
    cat /dev/urandom | tr -dc "\n[:alnum:]" | egrep '[[:alnum:]]{15,}' | colrm 16 | head -n 1000000 | sort > sorted${I}.txt;
done
