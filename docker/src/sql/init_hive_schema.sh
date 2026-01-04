#!/bin/bash
echo 'Waiting for HiveServer2 to be ready...'
for i in $(seq 1 30); do
  nc -z hive-server 10000 && break
  echo 'HiveServer2 not ready, waiting 10s...'
  sleep 10
done
echo 'Running ddl_hive.sql...'
beeline -u 'jdbc:hive2://hive-server:10000' -f /sql/ddl_hive.sql
echo 'Done!'
