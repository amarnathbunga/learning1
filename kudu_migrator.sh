#!/bin/bash

BASE_DIR="/mnt/vol1/dh-ads/joshkuduhive"
APPLICATION_JAR=${BASE_DIR}/jar/Josh.jar
export SPARK_HOME=${BASE_DIR}/spark-2.4.5-bin-hadoop2.7
export YARN_CONF_DIR=/etc/hive/conf

source ${BASE_DIR}/scripts/distcp/find_hw_namenode.sh
year=$(echo $(date -d "1 day ago"  +"%Y")|sed 's/^0*//')
month=$(echo $(date -d "1 day ago" +"%m")|sed 's/^0*//')
day=$(echo $(date -d "1 day ago" +"%d")|sed 's/^0*//')
minus_days=1
kudu_table_name="josh_nrt_ads_events_data"


$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --num-executors 4 --executor-cores 2 --executor-memory 4G --driver-memory 1G --conf "spark.executor.extraJavaOptions= -XX:+UseG1GC -XX:MaxGCPauseMillis=20  -XX:InitiatingHeapOccupancyPercent=50  -XX:G1HeapRegionSize=16M -XX:MinMetaspaceFreeRatio=50 -XX:MaxMetaspaceFreeRatio=80 -XX:-ResizePLAB -XX:ParallelGCThreads=5"  --conf "spark.hadoop.yarn.timeline-service.enabled=false" --conf "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2" --conf spark.debug.maxToStringFields=100 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.yarn.maxAppAttempts=1 --packages com.typesafe:config:1.2.1,org.apache.commons:commons-email:1.3,org.apache.kudu:kudu-spark2_2.11:1.10.0  --class in.dailyhunt.adtech.nrt.hivesinker.JoshDataMigrator ${APPLICATION_JAR} $minus_days "$kudu_table_name" $namenode

statusSpark=$?
echo "SPARK STATUS"
echo $statusSpark
if [ $statusSpark -eq 0 ]
then
        echo "Kudu T-1 day data moved to hwcluster successfully"
        ssh dh-ads@dh4-a3-hw-edge.dbp.dailyhunt.in 'hive -e "msck repair table josh_orc_ads_server_ad_request"'
	subject="[REPORT] Josh NRT data pipe: Kudu T-1 day data moved to hwcluster successfully $year-$month-$day"
        html_body="<html><body>Kudu T-1 day data moved to hwcluster successfully.</body></html>"
        java -cp "${APPLICATION_JAR}"  in.dailyhunt.utils.SendEmail "$subject" "$html_body"
        statusEmail3=$?
        if [ $statusEmail3 -eq 0 ]
        then
                echo "EMAIL SENT 1"
        else
                echo "EMAIL FAILED TO SEND 1"
        fi

else
        echo "Kudu T-1 day data failed to move into hwcluster"
        subject="[ALERT] Josh NRT data pipe: Kudu T-1 day data failed to move into hwcluster $year-$month-$day"
        html_body="<html><body>Kudu T-1 day data failed to move into hwcluster.</body></html>"
        java -cp "${APPLICATION_JAR}"  in.dailyhunt.utils.SendEmail "$subject" "$html_body"
        statusEmail3=$?
        if [ $statusEmail3 -eq 0 ]
        then
                echo "EMAIL SENT 2"
        else
                echo "EMAIL FAILED TO SEND 2"
        fi

fi

