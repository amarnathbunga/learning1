#!/usr/bin/env bash

########Import Hive Table data

source /mnt/vol1/dh-ads/kuduhive/scripts/distcp/find_hw_namenode.sh
echo $namenode
year=$(echo $(date  -d "6 day  ago" +"%Y")|sed 's/^0*//') 
month=$(echo $(date -d "6 day ago" +"%m")|sed 's/^0*//')
day=$(echo $(date  -d "6 day ago" +"%d")|sed 's/^0*//')
#IMPDIR="/orc-hive-staging/nrt_ads_events/properties_et_year=$year/properties_et_month=$month/properties_et_day=$day"
year=2020
month=8
day=5
LDIR="/orc-hive-staging/ads_server_ad_request/properties_et_year=$year/properties_et_month=$month/properties_et_day=$day"
echo $DIR
echo $year
echo $month
echo $day
#year=2020
#month=8
#day=5
hadoop distcp -D dfs.replication=1 -Dmapred.job.queue.name=default --update hdfs://$namenode:8020$LDIR hdfs://cdhadsnrt:8020$LDIR
#hdfs dfs -ls $IMPDIR | awk '{print $8}' | tail -n+2 | grep distcp  | xargs hdfs dfs -rm -r 
#hadoop distcp -D dfs.replication=1 --update  hdfs://192.168.3.190:8020/orc-hive-staging/ads_server_ad_request/properties_et_year=$year/properties_et_month=$month/properties_et_day=$day hdfs://dh-hw-ads-learning-n2.dailyhunt.in:8020/orc-hive-staging/ads_server_ad_request/properties_et_year=$year/properties_et_month=$month/properties_et_day=$day

#hive -e "alter table orc_ads_server_ad_request add partition ( properties_et_year=$year , properties_et_month=$month , properties_et_day=$day )"
#hive -e "alter table orc_ads_server_ad_request add partition ( properties_et_year=$year , properties_et_month=$month)"
