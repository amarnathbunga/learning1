#!/bin/bash

export SPARK_HOME=/mnt/vol1/dh-ads/joshkuduhive/spark-2.4.5-bin-hadoop2.7
export YARN_CONF_DIR=/etc/hive/conf
#cd "$(dirname "$0")"
current_year=$(date '+%Y' --date '-7 day')
current_month=$(date '+%m' --date '-7 day')
current_day=$(date '+%d' --date '-7 day')
kudu_table_name="josh_nrt_ads_events_data"
hive_table_name="josh_nrt_ads_events"
request_table_name="josh_nrt_ads_events_nofills"
josh_nofills_table_name="josh_nrt_ads_events_nofills"
echo $current_year
echo $current_month
echo $current_day

$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --num-executors 3 --executor-cores 2 --executor-memory 4G --driver-memory 1G  --files /mnt/vol1/dh-ads/joshkuduhive/scripts/application.properties  --conf "spark.executor.extraJavaOptions= -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=45 -XX:ConcGCThreads=2"  --conf spark.debug.maxToStringFields=100 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.yarn.maxAppAttempts=1 --packages com.typesafe:config:1.2.1,org.apache.commons:commons-email:1.3,org.apache.kudu:kudu-spark2_2.11:1.10.0 --class in.dailyhunt.adtech.nrt.hivesinker.JoshKuduHiveSinker  --conf spark.executor.memoryOverhead=1024  /mnt/vol1/dh-ads/joshkuduhive/jar/Josh.jar $current_year $current_month $current_day "$kudu_table_name" "$hive_table_name" 0 23 "cdhadsnrt" "$request_table_name"

statusSpark=$?
echo "SPARK STATUS"
echo $statusSpark
#statusSpark=0
if [ $statusSpark -eq 0 ]
then
	echo "SPARK SUBMIT SUCCESSFULL"
  #hadoop fs -setrep -R 1 /orc-hive-staging/$hive_table_name
  #hadoop fs -setrep -R 1 /orc-hive-staging/$request_table_name
	hive -e "msck repair table $hive_table_name sync partitions"
	statusHive=$?
	hive -e "msck repair table $josh_nofills_table_name sync partitions"
	statusHiveNofills=$?
	if [ $statusHive -eq 0 ] && [ $statusHiveNofills -eq 0 ]
	then
		echo "HIVE MSCK REPAIR SUCCESSFUL"
		impala-shell -V -i es-estimation-n11.internal.ads.dailyhunt.in -d default -q "invalidate metadata $hive_table_name"
		statusImpala=$?
		impala-shell -V -i es-estimation-n11.internal.ads.dailyhunt.in -d default -q "invalidate metadata $josh_nofills_table_name"
		statusImpalaNofills=$?
		if [ $statusImpala -eq 0 ] && [ $statusImpalaNofills -eq 0 ]
		then
			echo "IMPALA INVALIDATE METADATA SUCCESSFUL"
		else
			echo "IMPALA INVALIDATE METADATA FAILED"
			subject="ALERT : NRT data pipe: Invalidate metadata failed for $current_year-$current_month-$current_day"
			html_body="<html><body>Invalidate metadata for tables $hive_table_name and $josh_nofills_table_name has failed.</body></html>"
			java -cp "/mnt/vol1/dh-ads/joshkuduhive/jar/Josh.jar"  in.dailyhunt.utils.SendEmail "$subject" "$html_body"
			statusEmail1=$?
			if [ $statusEmail1 -eq 0 ]
			then
				echo "EMAIL SENT 1"
			else
				echo "EMAIL FAILED TO SEND 1"
			fi
			exit 1
		fi
	else
		echo "HIVE MSCK REPAIR FAILED"
		subject="ALERT : NRT data pipe: Msck Repair failed for $current_year-$current_month-$current_day"
		html_body="<html><body>Hive Msck repair for tables $hive_table_name and $josh_nofills_table_name has failed.</body></html>"
		java -cp "/mnt/vol1/dh-ads/joshkuduhive/jar/Josh.jar"  in.dailyhunt.utils.SendEmail "$subject" "$html_body"
		statusEmail2=$?
		if [ $statusEmail2 -eq 0 ]
		then
			echo "EMAIL SENT 2"
		else
			echo "EMAIL FAILED TO SEND 2"
		fi
		exit 1
	fi
else
        hive -e "ALTER TABLE $hive_table_name SET TBLPROPERTIES('EXTERNAL'='FALSE') ;"
	hive -e "ALTER TABLE $hive_table_name DROP IF EXISTS PARTITION (properties_et_year=$current_year , properties_et_month=$current_month , properties_et_day=$current_day , properties_et_hour=0);"
	for h in {1..23}
	do
	hive -e "ALTER TABLE $hive_table_name SET TBLPROPERTIES('EXTERNAL'='FALSE') ;"
	hive -e "ALTER TABLE $hive_table_name DROP IF EXISTS PARTITION (properties_et_year=$current_year , properties_et_month=$current_month , properties_et_day=$current_day , properties_et_hour=$h);"
	done
	hive -e "ALTER TABLE $hive_table_name SET TBLPROPERTIES('EXTERNAL'='TRUE') ;"
	impala-shell -V -i es-estimation-n11.internal.ads.dailyhunt.in -d default -q "invalidate metadata $hive_table_name"
	#echo "SPARK SUBMIT FAILED"
	sh /mnt/vol1/dh-ads/kuduhive/scripts/kudu_hive.sh
	#subject="ALERT : NRT data pipe: KuduHiveSinker Run for $current_year-$current_month-$current_day failed"
	#html_body="<html><body>Spark-Submit has failed.</body></html>"
	#java -cp "/mnt/vol1/dh-ads/kuduhive/jar/NrtDataProcessing-kudu_one_replica_wp-assembly-0.1.jar"  in.dailyhunt.utils.SendEmail "$subject" "$html_body"
	#statusEmail3=$?
	#if [ $statusEmail3 -eq 0 ]
	#then
	#	echo "EMAIL SENT 3"
	#else
	#	echo "EMAIL FAILED TO SEND 3"
	#fi
	exit 1
fi

