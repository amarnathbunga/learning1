#!/bin/bash

export JOSH_DIR=/mnt/vol1/dh-ads/joshkuduhive
export SPARK_HOME=${JOSH_DIR}/spark-2.4.5-bin-hadoop2.7
export YARN_CONF_DIR=/etc/hive/conf

current_year=$(date '+%Y' --date '-1 day')
current_month=$(date '+%m' --date '-1 day')
current_day=$(date '+%d' --date '-1 day')

$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --queue default --num-executors 5 --executor-cores 2 --executor-memory 2G --driver-memory 2G --files ${JOSH_DIR}/scripts/application.properties --conf "spark.executor.extraJavaOptions= -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=45 -XX:ConcGCThreads=2" --conf "spark.driver.extraJavaOptions=-Dhdp.version=3.1.0.0-78" --conf "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2" --conf "spark.hadoop.yarn.timeline-service.enabled=false" --conf spark.debug.maxToStringFields=100 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf "spark.executor.extraJavaOptions= -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=45 -XX:ConcGCThreads=2" --conf spark.debug.maxToStringFields=100 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --packages com.typesafe:config:1.2.1,org.apache.commons:commons-email:1.3,org.apache.kudu:kudu-spark2_2.11:1.10.0,org.mongodb.scala:mongo-scala-driver_2.11:2.9.0,mysql:mysql-connector-java:8.0.19,com.typesafe:config:1.2.1 --class in.dailyhunt.adtech.kudusinker.JoshUsersMongoKuduSinker --conf spark.executor.memoryOverhead=2048 --conf "spark.executor.extraJavaOptions=-XX:ThreadStackSize=3072" --conf "spark.sql.broadcastTimeout=36000" ${JOSH_DIR}/jar/AdsWeeklyProfileProcessing-assembly-0.1.jar $current_year $current_month $current_day
statusSpark=$?
echo "SPARK STATUS"
echo $statusSpark
if [ $statusSpark -eq 0 ]
then
        echo "SPARK SUBMIT SUCCESSFUL"
else
        echo "SPARK SUBMIT FAILED"
        subject="JoshUsersSinker failed for $current_year-$current_month-$current_day failed"
        html_body="<html><body>Spark-Submit has failed.</body></html>"
        java -cp "${JOSH_DIR}/jar/AdsWeeklyProfileProcessing-assembly-0.1.jar"  in.dailyhunt.utils.SendEmail "$subject" "$html_body"
fi

