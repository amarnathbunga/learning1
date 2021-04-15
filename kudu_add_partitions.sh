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
  subject="[Alert]: Josh NRT data pipe: Add range partitions failed"
	html_body1="<html><body>Josh Kudu add range partitions failed. Lower limit on number of days is not specified.</body></html>"
	java -cp "${APPLICATION_JAR}" in.dailyhunt.utils.SendEmail "$subject" "$html_body1"
  exit 1
fi

plus_year2=$(date '+%Y' --date '2 day')
plus_month2=$(date '+%m' --date '2 day')
plus_day2=$(date '+%d' --date '2 day')

plus_year3=$(date '+%Y' --date '3 day')
plus_month3=$(date '+%m' --date '3 day')
plus_day3=$(date '+%d' --date '3 day')

minus_year=$(date '+%Y' --date "-$line day")
minus_month=$(date '+%m' --date "-$line day")
minus_day=$(date '+%d' --date "-$line day")

minus_year2=$(date '+%Y' --date "-$(($line-1)) day")
minus_month2=$(date '+%m' --date "-$(($line-1)) day")
minus_day2=$(date '+%d' --date "-$(($line-1)) day")

partitionsAdded=""
partitionsNotAdded=""
partitionsDropped=""
partitionsNotDropped=""
countsUnmatched=""


#ADD PARTITIONS FOR +2 DAYS

queryAdd1="ALTER TABLE default.$kudu_table_name add range partition ($plus_year2, $plus_month2, $plus_day2, 0) <= VALUES < ($plus_year2, $plus_month2, $plus_day2, 12)"
parAdd1="PARTITION ($plus_year2, $plus_month2, $plus_day2, 0) <= VALUES < ($plus_year2, $plus_month2, $plus_day2, 12)"
echo $queryAdd1
echo $parAdd1
stderr=$(impala-shell -V -i es-estimation-n5.internal.ads.dailyhunt.in -d default -q "$queryAdd1" 2>&1 1>${BASE_DIR}/logs/kudu_partition_logs/logs )
statusAdd=$?
if [ $statusAdd -eq 0 ]
then
  echo "IMPALA QUERY 1 SUCCESSFUL"
  partitionsAdded+=$parAdd1"<br>"
else
  echo "IMPALA QUERY 1 UNSUCCESSFUL"
  error=$(echo $stderr | awk -F 'ERROR' '{ print $2 }')
  exception="ERROR"$error
  echo $exception
  partitionsNotAdded+=$parAdd1"<br>"$exception"<br>"
fi


queryAdd2="alter table default.$kudu_table_name add range partition ($plus_year2, $plus_month2, $plus_day2, 12) <= VALUES < ($plus_year3, $plus_month3, $plus_day3, 0)"
parAdd2="PARTITION ($plus_year2, $plus_month2, $plus_day2, 12) <= VALUES < ($plus_year3, $plus_month3, $plus_day3, 0)"
echo $queryAdd2
echo $parAdd2
stderr=$(impala-shell -V -i es-estimation-n5.internal.ads.dailyhunt.in -d default -q "$queryAdd2" 2>&1 1>${BASE_DIR}/logs/kudu_partition_logs/logs )
statusAdd=$?
if [ $statusAdd -eq 0 ]
then
  echo "IMPALA QUERY 3 SUCCESSFUL"
  partitionsAdded+=$parAdd2"<br>"
else
  echo "IMPALA QUERY 3 UNSUCCESSFUL"
  error=$(echo $stderr | awk -F 'ERROR' '{ print $2 }')
  exception="ERROR"$error
  echo $exception
  partitionsNotAdded+=$parAdd2"<br>"$exception"<br>"
fi



subject="[Report]: Josh NRT data pipe: Kudu Range Partitions Added"
html_body="<html><body>JOSH KUDU RANGE PARTITIONS<br>"
if [ -z "$partitionsAdded" ]
then
  echo "partitionsAdded empty"
else
  html_body+="<br>Following range partitions have been added:<br>"
  html_body+="$partitionsAdded"
fi

if [ -z "$partitionsNotAdded" ]
then
  echo "partitionsNotAdded empty"
else
  html_body+="<br>Following range partitions could not be added:<br>"
  html_body+="$partitionsNotAdded"
fi

html_body+="</body></html>"
echo $html_body

java -cp "${APPLICATION_JAR}" in.dailyhunt.utils.SendEmail "$subject" "$html_body"


