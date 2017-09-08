#!/bin/bash

if [[ $# -lt 1 ]] ; then
  echo "$0 <cmd-number> ?args?"
  exit -1
fi

case "$1" in 
  1)
    OPTS="-db perftest-0.db -numQueries 1000000 "
    OPTS+="-min 1 -step 5 -max 51 -querySize 1 -mmap "
    OPTS+="-customQuery  \"select * from perfTest;\" -numa -const"
    ;;
  2)
    OPTS="-db perftest-%d.db -numQueries 1000000 "
    OPTS+="-min 1 -step 5 -max 51 -querySize 1 -mmap "
    OPTS+="-customQuery  \"select * from perfTest;\" -numa -const"
    ;;
  *)
    echo "Invalid argument: $1"
    exit -1
esac

shift 1
echo "RUNNING: ./perftest $OPTS $@"
eval ./perftest "$OPTS" $@



