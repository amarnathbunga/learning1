#!/bin/bash

BASE_DIR="/mnt/vol1/dh-ads/joshkuduhive"
APPLICATION_JAR=${BASE_DIR}/jar/Josh.jar
export SPARK_HOME=${BASE_DIR}/spark-2.4.5-bin-hadoop2.7
export YARN_CONF_DIR=/etc/hive/conf

current_year=$(date '+%Y' --date '-1 hour')
current_month=$(date '+%m' --date '-1 hour')
current_day=$(date '+%d' --date '-1 hour')
current_hour=$(date '+%H' --date '-1 hour')
kudu_table_name="josh_nrt_ads_events_data"
hive_table_name="josh_nrt_ads_events"

$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --num-executors 1 --executor-cores 2 --executor-memory 2G --driver-memory 1G --conf "spark.executor.extraJavaOptions= -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=45 -XX:ConcGCThreads=2" --queue users.dh-ads --files ${BASE_DIR}/scripts/application.properties  --conf spark.debug.maxToStringFields=100 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --packages com.typesafe:config:1.2.1,org.apache.commons:commons-email:1.3,org.apache.kudu:kudu-spark2_2.11:1.10.0,mysql:mysql-connector-java:8.0.19 --class in.dailyhunt.monitoring.JoshNrtKuduCounts --conf spark.executor.memory=4G  --conf spark.executor.memoryOverhead=1024 ${APPLICATION_JAR}

statusSpark=$?
echo "SPARK STATUS"
echo $statusSpark
if [ $statusSpark -eq 0 ]
then
	echo "SPARK SUBMIT SUCCESSFULL"
else
	echo "SPARK SUBMIT FAILED"
	subject="[Alert]: Josh NRT data pipe: JoshNrtKuduCounts failed for $current_year-$current_month-$current_day-$current_hour failed"
	html_body="<html><body>Spark-Submit has failed.</body></html>"
#	java -cp "${APPLICATION_JAR}"  in.dailyhunt.utils.SendEmail "$subject" "$html_body"
fi

