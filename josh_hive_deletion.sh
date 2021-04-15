#!/bin/bash

export SPARK_HOME=/mnt/vol1/dh-ads/joshkuduhive/spark-2.4.5-bin-hadoop2.7
export YARN_CONF_DIR=/etc/hive/conf
delete_year=$(echo $(date -d "15 day ago"  +"%Y")|sed 's/^0*//')
delete_month=$(echo $(date -d "15 day ago" +"%m")|sed 's/^0*//')
delete_day=$(echo $(date -d "15 day ago" +"%d")|sed 's/^0*//')
hive_table_name="josh_nrt_ads_events"
hive_clicks="josh_hive_clicks"

$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --num-executors 10 --executor-cores 2 --executor-memory 3G --driver-memory 1G  --files /mnt/vol1/dh-ads/joshkuduhive/scripts/application.properties  --conf "spark.executor.extraJavaOptions= -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=45 -XX:ConcGCThreads=2"  --conf spark.debug.maxToStringFields=100 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --packages com.typesafe:config:1.2.1,org.apache.commons:commons-email:1.3,org.apache.kudu:kudu-spark2_2.11:1.10.0 --class in.dailyhunt.adtech.nrt.hivesinker.HiveDeletion  --conf spark.executor.memoryOverhead=1024  /mnt/vol1/dh-ads/joshkuduhive/jar/Josh.jar $delete_year $delete_month $delete_day "$hive_table_name" "$hive_clicks" "cdhadsnrt"

statusSpark=$?
echo "SPARK STATUS"
echo $statusSpark
if [ $statusSpark -eq 0 ]
then
	echo "SPARK SUBMIT SUCCESSFULL"
        hdfs_path="/orc-hive-staging/josh_hive_clicks/event_year=$delete_year/event_month=$delete_month/event_day=$delete_day"
	original_path="/orc-hive-staging/josh_nrt_ads_events/event_year=$delete_year/event_month=$delete_month/event_day=$delete_day"

      	hadoop fs -test -e $hdfs_path
	if [ $? == 0 ]
    	    then
		hadoop fs -rm -r  $original_path 
        	hive -e "msck repair table josh_nrt_ads_events sync partitions"
                impala-shell -V -i es-estimation-n11.internal.ads.dailyhunt.in -d default -q "invalidate metadata josh_nrt_ads_events"
		hadoop fs -cp -f $hdfs_path $original_path
        	#hadoop fs -setrep -R 1 $original_path
	fi
	hive -e "msck repair table $hive_table_name sync partitions"
	statusHive=$?
	if [ $statusHive -eq 0 ]
	then
		echo "HIVE MSCK REPAIR SUCCESSFUL"
		hive -e "show partitions josh_nrt_ads_events" > /mnt/vol1/dh-ads/joshkuduhive/scripts/hive_deletion/partitions.txt
                a_day=$(head -1 /mnt/vol1/dh-ads/joshkuduhive/scripts/hive_deletion/partitions.txt | cut -d/ -f3)
                day=$(echo $a_day | cut -d= -f2)
                a_month=$(head -1 /mnt/vol1/dh-ads/joshkuduhive/scripts/hive_deletion/partitions.txt | cut -d/ -f2)
                month=$(echo $a_month | cut -d= -f2)
                a_year=$(head -1 /mnt/vol1/dh-ads/joshkuduhive/scripts/hive_deletion/partitions.txt | cut -d/ -f1)
                year=$(echo $a_year | cut -d= -f2)
                kudu_year=$(echo $(date -d "5 day ago"  +"%Y")|sed 's/^0*//')
                kudu_month=$(echo $(date -d "5 day ago" +"%m")|sed 's/^0*//')
                kudu_day=$(echo $(date -d "5 day ago" +"%d")|sed 's/^0*//')
                start_year=$(echo $(date -d "14 day ago"  +"%Y")|sed 's/^0*//')
                start_month=$(echo $(date -d "14 day ago" +"%m")|sed 's/^0*//')
                start_day=$(echo $(date -d "14 day ago" +"%d")|sed 's/^0*//')
                end_year=$(echo $(date -d "6 day ago"  +"%Y")|sed 's/^0*//')
                end_month=$(echo $(date -d "6 day ago" +"%m")|sed 's/^0*//')
                end_day=$(echo $(date -d "6 day ago" +"%d")|sed 's/^0*//')

                echo "postimpressions data available from $day-$month-$year to $delete_day-$delete_month-$delete_year"
                echo "impressions data available from $start_day-$start_month-$start_year to $end_day-$end_month-$end_year"
                echo "kudu josh_nrt_ads_events_view data avalible from $kudu_day-$kudu_month-$kudu_year"
                subject="[REPORT] JOSH NRT data pipe: Data Available From $day-$month-$year"
                html_body="<html>
                <body>
		<p>Kudu: All Events: $kudu_day-$kudu_month-$kudu_year onwards</p>
		<p>Hive: Impressions and post impression events: $start_day-$start_month-$start_year to $end_day-$end_month-$end_year</p>
                <p>Hive: Clicks and post clicks events: $day-$month-$year to $delete_day-$delete_month-$delete_year</p>
                </body>
                </html>"
                java -cp "/mnt/vol1/dh-ads/joshkuduhive/jar/Josh.jar"  in.dailyhunt.utils.SendEmail "$subject" "$html_body"
                statusEmail4=$?
                if [ $statusEmail4 -eq 0 ]
                then
                        echo "EMAIL SENT 4"
                else
                        echo "EMAIL FAILED TO SEND 4"
                fi

		impala-shell -V -i es-estimation-n11.internal.ads.dailyhunt.in -d default -q "invalidate metadata $hive_table_name"
		statusImpala=$?
		if [ $statusImpala -eq 0 ]
		then
			echo "IMPALA INVALIDATE METADATA SUCCESSFUL"
		else
			echo "IMPALA INVALIDATE METADATA FAILED"
			subject="JOSH NRT data pipe: Hive Deletion Invalidate metadata failed for $delete_year-$delete_month-$delete_day"
			html_body="<html><body>Invalidate metadata for table $hive_table_name has failed.</body></html>"
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
		subject="JOSH NRT data pipe: Hive Deletion Msck Repair failed for $delete_year-$delete_month-$delete_day"
		html_body="<html><body>Hive Msck repair for table $hive_table_name has failed.</body></html>"
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
	echo "SPARK SUBMIT FAILED"
	subject="JOSH NRT data pipe: Hive Deletion Run for $delete_year-$delete_month-$delete_day failed"
	html_body="<html><body>Spark-Submit for Hive Deletion has failed.</body></html>"
	java -cp "/mnt/vol1/dh-ads/joshkuduhive/jar/Josh.jar"  in.dailyhunt.utils.SendEmail "$subject" "$html_body"
	statusEmail3=$?
	if [ $statusEmail3 -eq 0 ]
	then
		echo "EMAIL SENT 3"
	else
		echo "EMAIL FAILED TO SEND 3"
	fi
	exit 1
fi
