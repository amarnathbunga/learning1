#!/bin/bash

BASE_DIR="/mnt/vol1/dh-ads/joshkuduhive"
APPLICATION_JAR=${BASE_DIR}/jar/Josh.jar
kudu_table_name="josh_nrt_ads_events_data"
hive_table_name="josh_nrt_ads_events"
filename="${BASE_DIR}/scripts/days.properties"
line=$(head -n 1 $filename)
if [ -z $line ]
then
  echo "NUMBER OF DAYS EMPTY"
  subject="[Alert]: Josh NRT data pipe: Impala Alter view failed"
  html_body1="<html><body>Josh Impala Alter view failed. Lower limit on number of days is not specified.</body></html>"
  java -cp "${APPLICATION_JAR}" in.dailyhunt.utils.SendEmail "$subject" "$html_body1"
  exit 1
fi

current_year=$(date '+%Y' --date "-$line day")
current_month=$(date '+%m' --date "-$line day")
current_day=$(date '+%d' --date "-$line day")

for hour in {0..23} ; do echo $hour
#CHECK IF HIVE COUNT MATCHES KUDU COUNT
impala-shell -V -i es-estimation-n5.internal.ads.dailyhunt.in -d default -q "refresh $hive_table_name"
hive_count=$(impala-shell -V -i es-estimation-n5.internal.ads.dailyhunt.in -d default -q "select count(*) from $hive_table_name where event_year=$current_year and event_month=$current_month and event_day=$current_day and event_hour=$hour")
kudu_count=$(impala-shell -V -i es-estimation-n5.internal.ads.dailyhunt.in -d default -q "select count(*) from $kudu_table_name where event_year=$current_year and event_month=$current_month and event_day=$current_day and event_hour=$hour and (has_impression = 1 or has_click = 1 or has_install = 1 or has_lead = 1 or has_postinstall = 1 or has_vast = 1)")

harray=( $( for i in $hive_count ; do echo $i ; done ) )
hcnt=${#harray[@]}
echo "##########HIVE COUNT#######"
echo "${harray[6]}"

array=( $( for i in $kudu_count ; do echo $i ; done ) )
cnt=${#array[@]}
echo "##########KUDU COUNT#######"
echo "${array[6]}"


count_percent=$(echo "(${array[6]}-${harray[6]})*100/${harray[6]}" | bc -l )
echo $count_percent
if [ $count_percent > 1 ]
then
  echo "COUNTS DID NOT MATCH for hour $hour"
  #send email here
  subject="[Alert]: Josh NRT data pipe: Kudu Hive difference is greater than 1 percent for hour $hour $current_year-$current_month-$current_day-$hour"
  html_body="<html><body>Josh Kudu Hive difference is greater than 1 percent for hour $hour "$current_year-$current_month-$current_day-$hour" </body></html>"
  java -cp "${APPLICATION_JAR}" in.dailyhunt.utils.SendEmail "$subject" "$html_body"
fi
done

