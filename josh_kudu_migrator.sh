#!/bin/bash

BASE_DIR="/mnt/vol1/dh-ads/joshkuduhive"
APPLICATION_JAR=${BASE_DIR}/jar/Josh.jar
export SPARK_HOME=${BASE_DIR}/spark-2.4.5-bin-hadoop2.7
export YARN_CONF_DIR=/etc/hive/conf

source ${BASE_DIR}/scripts/distcp/find_hw_namenode.sh

utcTime=$1
year=$(echo $utcTime | cut -d - -f 1 | sed 's/^0*//')
month=$(echo $utcTime | cut -d - -f 2 | sed 's/^0*//')
day=$(echo $utcTime | cut -d T -f 1 | cut -d - -f 3 | sed 's/^0*//')
hour=$(echo $utcTime | cut -d T -f 2 | cut -d : -f 1 | sed 's/^0//')

#year=2021
#month=3
#day=31
#hour=21

kudu_table_name="josh_nrt_ads_events_data"
hwcluster_dir="josh_nrt_ads_events"
warehouse="hdfs://$namenode:8020/user/dh-ads/$hwcluster_dir"
partition_year="event_year"
partition_month="event_month"
partition_day="event_day"
partition_hour="event_hour"
echo $namenode
echo $year
echo $month
echo $day
echo $hour
echo $kudu_table_name
echo $hwcluster_dir
echo $warehouse
echo $partition_year
echo $partition_month
echo $partition_day
echo $partition_hour


$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --num-executors 4 --executor-cores 3 --executor-memory 3G --driver-memory 1G --conf "spark.executor.extraJavaOptions= -XX:+UseG1GC -XX:MaxGCPauseMillis=20  -XX:InitiatingHeapOccupancyPercent=50  -XX:G1HeapRegionSize=16M -XX:MinMetaspaceFreeRatio=50 -XX:MaxMetaspaceFreeRatio=80 -XX:-ResizePLAB -XX:ParallelGCThreads=5"  --conf "spark.hadoop.yarn.timeline-service.enabled=false" --conf "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2" --conf spark.debug.maxToStringFields=100 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.yarn.maxAppAttempts=1 --packages com.typesafe:config:1.2.1,org.apache.commons:commons-email:1.3,org.apache.kudu:kudu-spark2_2.11:1.10.0  --class in.dailyhunt.adtech.nrt.hivesinker.Migrator ${APPLICATION_JAR} $year $month $day $hour "$kudu_table_name" $warehouse $partition_year $partition_month $partition_day $partition_hour

statusSpark=$?
echo "SPARK STATUS"
echo $statusSpark
if [ $statusSpark -eq 0 ]
then
        echo "Kudu hour data moved to hwcluster successfully"
        ssh dh-ads@dh4-a3-hw-edge.dbp.dailyhunt.in 'hive -e "msck repair table josh_orc_ads_server_ad_request"'
        exit $?
       #subject="[REPORT] Josh NRT data pipe: Kudu hourly data moved to hwcluster successfully $year-$month-$day-$hour"
       # html_body="<html><body>Kudu hourly data moved to hwcluster successfully.</body></html>"
       # java -cp "${APPLICATION_JAR}"  in.dailyhunt.utils.SendEmail "$subject" "$html_body"
       # statusEmail3=$?
       # if [ $statusEmail3 -eq 0 ]
       # then
       #         echo "EMAIL SENT 1"
       # else
       #         echo "EMAIL FAILED TO SEND 1"
       # fi

else
        echo "Kudu hour data failed to move into hwcluster"
        #subject="[ALERT] Josh NRT data pipe: Kudu hourly data failed to move into hwcluster $year-$month-$day-$hour"
        #html_body="<html><body>Kudu hourly data failed to move into hwcluster.</body></html>"
        #java -cp "${APPLICATION_JAR}"  in.dailyhunt.utils.SendEmail "$subject" "$html_body"
        #statusEmail3=$?
        #if [ $statusEmail3 -eq 0 ]
        #then
        #        echo "EMAIL SENT 2"
        #else
        #        echo "EMAIL FAILED TO SEND 2"
        #fi
        exit $statusSpark

fi
