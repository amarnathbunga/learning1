#!/bin/bash

BASE_DIR="/mnt/vol1/dh-ads/joshkuduhive"
#JAR_DIR="/mnt/vol1/dh-ads/joshkuduhive"
#APPLICATION_JAR=${BASE_DIR}/jar/NrtDataProcessing-kudu_one_replica_wp-assembly-0.1.jar
APPLICATION_JAR=${BASE_DIR}/jar/test.jar
export SPARK_HOME=${BASE_DIR}/spark-2.4.5-bin-hadoop2.7
export YARN_CONF_DIR=/etc/hive/conf

utcTime=$1
year=$(echo $utcTime | cut -d - -f 1 | sed 's/^0*//')
month=$(echo $utcTime | cut -d - -f 2 | sed 's/^0*//')
day=$(echo $utcTime | cut -d T -f 1 | cut -d - -f 3 | sed 's/^0*//')
hour=$(echo $utcTime | cut -d T -f 2 | cut -d : -f 1 | sed 's/^0//')

#year=2021
#month=4
#day=7
#hour=16

kudu_table_name="josh_nrt_ads_events_data"
requests_table="josh_nrt_ads_events_nofills"
impressions_table="josh_nrt_ads_events"
requestswarehouse="hdfs://cdhadsnrt:8020/orc-hive-staging/$requests_table"
impressionswarehouse="hdfs://cdhadsnrt:8020/orc-hive-staging/$impressions_table"
hive_requests_table="josh_nrt_ads_events_nofills"
partition_year="event_year"
partition_month="event_month"
partition_day="event_day"
partition_hour="event_hour"
echo $year
echo $month
echo $day
echo $hour
echo $kudu_table_name
echo $requests_table
echo $impressions_table
echo $requestswarehouse
echo $impressionswarehouse
echo $partition_year
echo $partition_month
echo $partition_day
echo $partition_hour

impquery="where $partition_year=$year and $partition_month=$month and $partition_day=$day and $partition_hour=$hour and (has_impression = 1 or has_click = 1 or has_install = 1 or has_lead = 1 or has_postinstall = 1 or has_vast = 1)"
echo "${impquery}"
requestquery="where $partition_year=$year and $partition_month=$month and $partition_day=$day and $partition_hour=$hour and has_request = 1 and has_impression is null and has_click is null and has_install is null and has_lead is null and has_postinstall is null and has_vast is null"
echo "${requestquery}"

$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --num-executors 10 --executor-cores 2 --executor-memory 3G --driver-memory 1G --conf "spark.executor.extraJavaOptions= -XX:+UseG1GC -XX:MaxGCPauseMillis=20  -XX:InitiatingHeapOccupancyPercent=50  -XX:G1HeapRegionSize=16M -XX:MinMetaspaceFreeRatio=50 -XX:MaxMetaspaceFreeRatio=80 -XX:-ResizePLAB -XX:ParallelGCThreads=5" --files ${BASE_DIR}/scripts/application.properties --conf spark.debug.maxToStringFields=100 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.yarn.maxAppAttempts=1 --packages com.typesafe:config:1.2.1,org.apache.commons:commons-email:1.3,org.apache.kudu:kudu-spark2_2.11:1.10.0  --class in.dailyhunt.adtech.nrt.hivesinker.JoshHiveSinker ${APPLICATION_JAR} $year $month $day $hour $kudu_table_name $requestswarehouse $impressionswarehouse $partition_year $partition_month $partition_day $partition_hour $impressions_table "${impquery}" "${requestquery}"

statusSpark=$?
echo "SPARK STATUS"
echo $statusSpark
if [ $statusSpark -eq 0 ]
then
	echo "SPARK SUBMIT SUCCESSFULL"
	hive -e "set hive.msck.path.validation=ignore; msck repair table $impressions_table sync partitions"
	statusHive=$?
	hive -e "set hive.msck.path.validation=ignore; msck repair table $hive_requests_table sync partitions"
	statusHiveNofills=$?
	if [ $statusHive -eq 0 ] && [ $statusHiveNofills -eq 0 ]
	then
		echo "HIVE MSCK REPAIR SUCCESSFUL"
        else
                echo "HIVE MSCK REPAIR FAILED"
        fi
        exit $statusSpark
else
	echo "SPARK SUBMIT FAILED"
        exit $statusSpark
fi

