#!/bin/bash

export JOSH_DIR=/mnt/vol1/dh-ads/joshkuduhive
export SPARK_HOME=${JOSH_DIR}/spark-2.4.5-bin-hadoop2.7
export YARN_CONF_DIR=/etc/hive/conf


mongo_collection_name="JoshAdProfile_Week_"
#compute week number starting from Sunday
week_number=11

mysql -uoads_user -peterno123 -hdb-slave1.internal.ads.dailyhunt.in -P3306 daedalus<<EOFMYSQL
	insert into josh_kudu_mongo_sinker_hourly_ops (job_name, current_year, current_month, current_day, current_hour, next_year, next_month, next_day, next_hour, week_number, is_processed, retries) values ("JoshKuduMongoSinker", $current_year, $current_month, $current_day, $current_hour, $next_year, $next_month, $next_day, $next_hour, $week_number, 0, 1);
EOFMYSQL

$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --queue nrt --num-executors 3 --executor-cores 2 --executor-memory 1G --driver-memory 1G  --files ${JOSH_DIR}/scripts/application.properties --conf "spark.executor.extraJavaOptions= -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=45 -XX:ConcGCThreads=2" --conf spark.debug.maxToStringFields=100 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --packages com.typesafe:config:1.2.1,org.apache.commons:commons-email:1.3,org.apache.kudu:kudu-spark2_2.11:1.10.0,mysql:mysql-connector-java:8.0.19,org.mongodb.scala:mongo-scala-driver_2.11:2.7.0 --class in.dailyhunt.adtech.mongosinker.JoshHiveMongoSinker --conf spark.executor.memoryOverhead=1024 ${JOSH_DIR}/jar/AdsWeeklyProfileProcessing-assembly-0.1.jar 2021 3 21 17 "$week_number" "$mongo_collection_name" "dh-hw-m1nn1.dbp.dailyhunt.in:8020"

statusSpark=$?
echo "SPARK STATUS"
echo $statusSpark
if [ $statusSpark -eq 0 ]
then
        echo "SPARK SUBMIT SUCCESSFUL"
else
        echo "SPARK SUBMIT FAILED"
        subject="JoshKuduMongoSinker failed for $current_year-$current_month-$current_day-$current_hour failed"
        html_body="<html><body>Spark-Submit has failed.</body></html>"
  #      java -cp "${JOSH_DIR}/jar/AdsWeeklyProfileProcessing-assembly-0.1.jar"  in.dailyhunt.utils.SendEmail "$subject" "$html_body"
fi


