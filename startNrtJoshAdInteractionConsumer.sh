#!/bin/bash

export JOSH_DIR=/mnt/vol1/dh-ads/joshkuduhive
export SPARK_HOME=${JOSH_DIR}/spark-2.4.5-bin-hadoop2.7
export YARN_CONF_DIR=/etc/hive/conf

$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --conf "spark.executor.extraJavaOptions= -XX:+UseG1GC -XX:MaxGCPauseMillis=20  -XX:InitiatingHeapOccupancyPercent=50  -XX:G1HeapRegionSize=64M -XX:MinMetaspaceFreeRatio=50 -XX:MaxMetaspaceFreeRatio=80 -XX:-ResizePLAB -XX:ParallelGCThreads=24" --queue nrt --num-executors 3 --executor-cores 2 --executor-memory 1G --driver-memory 1G  --files ${JOSH_DIR}/scripts/application.properties --conf "spark.dynamicAllocation.executorIdleTimeout=600s" --conf "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2"  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4,com.typesafe:config:1.2.1,org.apache.commons:commons-email:1.3,org.apache.kudu:kudu-spark2_2.11:1.10.0  --class in.dailyhunt.adtech.nrt.interactionevents.NrtJoshAdInteractionConsumer ${JOSH_DIR}/jar/Josh.jar --conf spark.debug.maxToStringFields=100 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.yarn.maxAppAttempts=2

status=$?
if [ $status -ne 0 ]
then
  echo "spark submit failed"
  subject="[Alert] NrtJoshAdInteractionConsumer failed"
  html_body="<html><body>Spark job for NrtJoshAdInteractionConsumer has failed.</body></html>"
#  java -cp "${JOSH_DIR}/jar/Josh.jar" in.dailyhunt.utils.SendEmail "$subject" "$html_body"
fi

