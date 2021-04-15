#!/bin/bash

export BASE_DIR=/mnt/vol1/dh-ads/joshkuduhive
export SPARK_HOME=${BASE_DIR}/spark-2.4.5-bin-hadoop2.7
export YARN_CONF_DIR=/etc/hive/conf

export APPLICATION_JAR=${BASE_DIR}/jar/Josh.jar

$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --queue nrt --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --files ${BASE_DIR}/scripts/application.properties --conf "spark.hadoop.yarn.timeline-service.enabled=false" --conf "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2"  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4,com.typesafe:config:1.2.1,org.apache.commons:commons-email:1.3,org.apache.kudu:kudu-spark2_2.11:1.10.0 --class JoshResponseProcessorMain ${APPLICATION_JAR} --conf spark.debug.maxToStringFields=100 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer

status=$?

if [ $status -ne 0 ]
then
  echo "spark submit failed"
  subject="Josh NRT data pipe: Kudu Josh Ad Response Processor failed"
        html_body="<html><body>Spark job for Kafka to kudu processor for Josh Ad Response has failed.</body></html>"
        #java -cp "${APPLICATION_JAR}"  in.dailyhunt.utils.SendEmail "$subject" "$html_body"
fi

