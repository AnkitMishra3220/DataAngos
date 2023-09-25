#!/bin/bash

spark-submit --master yarn \
--deploy-mode cluster \
--name "DataAngos" \
--conf spark.executor.instances=4 \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.executor.cores=2 \
--conf spark.driver.memory=2g \
--conf spark.executor.memory=5g \
--conf spark.driver.cores=2 \
--conf spark.serializer=org.apache.spark.seriacp lizer.KryoSerializer \
--files /home/hadoop/application.yml \
--class com.da.digital.Application \
/home/hadoop/dataangos-1.0-SNAPSHOT.jar
