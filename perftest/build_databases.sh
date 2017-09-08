#!/bin/bash

if [[ $# -ne 2 ]] ; then
  echo "$0 <sql-script> <number-of-dbs>"
  echo ""
  echo "This script creates <number-of-dbs> databases named \"perftest-\$i.db\","
  echo "where \$i is an integer between 0 and (<number-of-dbs>-1). Any existing"
  echo "files are clobbered. Each database is created by running the commands:"
  echo ""
  echo "  rm -f perftest-\$i.db"
  echo "  cat <sql-script> | sqlite3 perftest-\$i.db"
  echo ""
  exit -1
fi

SQL=$1
NDB=$2

for ((i=0; i<$NDB; i++))
do
  DB=perftest-$i.db
  echo $DB
  rm -f $DB
  sqlite3 $DB < $SQL
done


