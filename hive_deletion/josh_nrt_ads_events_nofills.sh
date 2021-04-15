#!/usr/bin/env bash

########Import Hive Table data

YEAR=$(echo $(date -d "15 day ago"  +"%Y")|sed 's/^0*//') 
MONTH=$(echo $(date -d "15 day ago" +"%m")|sed 's/^0*//')
DAY=$(echo $(date -d "15 day ago" +"%d")|sed 's/^0*//')
DAY_NEXT=$(echo $(date -d "14 day ago" +"%d")|sed 's/^0*//')

echo $YEAR
echo $MONTH
echo $DAY
echo $DAY_NEXT
#DIR="/orc-hive-staging/nrt_ads_events/properties_et_year=$YEAR/properties_et_month=$MONTH/properties_et_day=$DAY/properties_et_hour=$HOUR"
#echo $DIR

#hdfs dfs -rm -r -skipTrash $DIR

#for HOUR in $(seq 0 23); do  
#DIR="/orc-hive-staging/nrt_ads_events/properties_et_year=$YEAR/properties_et_month=$MONTH/properties_et_day=$DAY/properties_et_hour=$HOUR"
#echo $DIR
#hadoop fs -rm -r -skipTrash $DIR
#hive -e "alter table nrt_ads_events drop partition(properties_et_year=$YEAR , properties_et_month=$MONTH , properties_et_day=$DAY, properties_et_hour=$HOUR ) ;"
#hive -e "msck repair table nrt_ads_events sync partitions;"
#done

hadoop fs -rm -r -skipTrash /orc-hive-staging/josh_nrt_ads_events_nofills/event_year=$YEAR/event_month=$MONTH/event_day=$DAY                  
if [ $DAY_NEXT -eq 1 ]
then 
  hadoop fs -rm -r -skipTrash /orc-hive-staging/josh_nrt_ads_events_nofills/event_year=$YEAR/event_month=$MONTH
else
  echo "no"
fi 
hive -e "msck repair table josh_nrt_ads_events_nofills sync partitions"
impala-shell -V -i es-estimation-n11.internal.ads.dailyhunt.in -d default -q "invalidate metadata josh_nrt_ads_events_nofills"
