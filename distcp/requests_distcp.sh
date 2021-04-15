#!/usr/bin/env bash

########Import Hive Table data

source /mnt/vol1/dh-ads/joshkuduhive/scripts/distcp/find_hw_namenode.sh
echo $namenode
year=$(echo $(date  -d "6 day  ago" +"%Y")|sed 's/^0*//') 
month=$(echo $(date -d "6 day ago" +"%m")|sed 's/^0*//')
day=$(echo $(date  -d "6 day ago" +"%d")|sed 's/^0*//')
REQDIR="/orc-hive-staging/josh_nrt_ads_events_nofills/properties_et_year=$year/properties_et_month=$month/properties_et_day=$day"
#LDIR="/orc-hive-staging/ads_server_ad_request/properties_et_year=$year/properties_et_month=$month"
echo $REQDIR
echo $year
echo $month
echo $day
hadoop distcp -D dfs.replication=2 -Dmapred.job.queue.name=nrt --update hdfs://cdhadsnrt:8020$REQDIR hdfs://$namenode:8020$REQDIR
#hdfs dfs -ls $IMPDIR | awk '{print $8}' | tail -n+2 | grep distcp  | xargs hdfs dfs -rm -r 
#hadoop distcp -D dfs.replication=1 --update  hdfs://192.168.3.190:8020/orc-hive-staging/ads_server_ad_request/properties_et_year=$year/properties_et_month=$month/properties_et_day=$day hdfs://dh-hw-ads-learning-n2.dailyhunt.in:8020/orc-hive-staging/ads_server_ad_request/properties_et_year=$year/properties_et_month=$month/properties_et_day=$day

#hive -e "alter table orc_ads_server_ad_request add partition ( properties_et_year=$year , properties_et_month=$month , properties_et_day=$day )"
#hive -e "alter table orc_ads_server_ad_request add partition ( properties_et_year=$year , properties_et_month=$month)"
