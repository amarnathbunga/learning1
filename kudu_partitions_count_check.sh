#!/bin/bash

BASE_DIR="/mnt/vol1/dh-ads/joshkuduhive"
APPLICATION_JAR=${BASE_DIR}/jar/Josh.jar
kudu_table_name="josh_nrt_ads_events_data"
hive_table_name="josh_nrt_ads_events"
req_hive_table_name="josh_nrt_ads_events_nofills"

filename="${BASE_DIR}/scripts/days.properties"
line=$(head -n 1 $filename)
if [ -z $line ]
then
  echo "NUMBER OF DAYS EMPTY"
  subject="[Alert]: Josh NRT data pipe: Drop range partitions failed"
	html_body1="<html><body>Kudu drop range partitions failed. Lower limit on number of days is not specified.</body></html>"
	java -cp "${APPLICATION_JAR}" in.dailyhunt.utils.SendEmail "$subject" "$html_body1"
  exit 1
fi

plus_year2=$(date '+%Y' --date '1 day')
plus_month2=$(date '+%m' --date '1 day')
plus_day2=$(date '+%d' --date '1 day')

plus_year3=$(date '+%Y' --date '2 day')
plus_month3=$(date '+%m' --date '2 day')
plus_day3=$(date '+%d' --date '2 day')

minus_year=$(date '+%Y' --date "-$line day")
minus_month=$(date '+%m' --date "-$line day")
minus_day=$(date '+%d' --date "-$line day")

minus_year2=$(date '+%Y' --date "-$(($line-1)) day")
minus_month2=$(date '+%m' --date "-$(($line-1)) day")
minus_day2=$(date '+%d' --date "-$(($line-1)) day")

partitionsDropped=""
partitionsNotDropped=""
countsUnmatched=""

#CHECK IF HIVE COUNT MATCHES KUDU COUNT
impala-shell -V -i es-estimation-n5.internal.ads.dailyhunt.in -d default -q "refresh $hive_table_name"
impala-shell -V -i es-estimation-n5.internal.ads.dailyhunt.in -d default -q "refresh $req_hive_table_name"
#TODO: Add ad_interaction_count = 1 or similar
imp_hive_count=$(impala-shell -V -i es-estimation-n5.internal.ads.dailyhunt.in -d default -q "select count(*) from $hive_table_name where event_year=$minus_year and event_month=$minus_month and event_day=$minus_day")
req_hive_count=$(impala-shell -V -i es-estimation-n5.internal.ads.dailyhunt.in -d default -q "select count(*) from $req_hive_table_name where event_year=$minus_year and event_month=$minus_month and event_day=$minus_day")
imp_kudu_count=$(impala-shell -V -i es-estimation-n5.internal.ads.dailyhunt.in -d default -q "select count(*) from $kudu_table_name where event_year=$minus_year and event_month=$minus_month and event_day=$minus_day and (has_impression = 1 or has_click = 1 or has_install = 1 or has_lead = 1 or has_postinstall = 1 or has_vast = 1)")
req_kudu_count=$(impala-shell -V -i es-estimation-n5.internal.ads.dailyhunt.in -d default -q "select count(*) from $kudu_table_name where event_year=$minus_year and event_month=$minus_month and event_day=$minus_day and has_request = 1 and has_impression is null and has_click is null and has_install is null and has_lead is null and has_postinstall is null and has_vast is null")


imp_harray=( $( for i in $imp_hive_count ; do echo $i ; done ) )
imp_hcnt=${#imp_harray[@]}
echo "##########IMP HIVE COUNT#######"
echo "${imp_harray[6]}"

imp_array=( $( for i in $imp_kudu_count ; do echo $i ; done ) )
imp_cnt=${#imp_array[@]}
echo "##########IMP KUDU COUNT#######"
echo "${imp_array[6]}"

imp_count_percent=$(echo "(${imp_array[6]}-${imp_harray[6]})*100/${imp_harray[6]}" | bc -l )


req_harray=( $( for i in $req_hive_count ; do echo $i ; done ) )
req_hcnt=${#req_harray[@]}
echo "##########REQ HIVE COUNT#######"
echo "${req_harray[6]}"

req_array=( $( for i in $req_kudu_count ; do echo $i ; done ) )
req_cnt=${#req_array[@]}
echo "##########REQ KUDU COUNT#######"
echo "${req_array[6]}"

req_count_percent=$(echo "(${req_array[6]}-${req_harray[6]})*100/${req_harray[6]}" | bc -l )
echo $imp_count_percent
echo $req_count_percent

#if [ $imp_count_percent < 1 ] && [ $req_count_percent < 1 ] ;
#then
#    echo "amar"
#else 
#    echo"abhi"
#fi

#if [ "$imp_count_percent" -lt 1 ] && [ "$req_count_percent" -lt 1 ]; then
#    echo "Test Done !"
#else
#    echo "Test Failed !"
#fi

if [ $imp_count_percent -lt 1 ] && [ $req_count_percent < 1 ]
then
  echo "##################"
  #DELETE PARTITIONS FOR -5 DAYS
  queryDrop1="alter table default.$kudu_table_name drop range partition ($minus_year, $minus_month, $minus_day, 0) <= VALUES < ($minus_year, $minus_month, $minus_day, 12)"
  parDrop1="PARTITION ($minus_year, $minus_month, $minus_day, 0) <= VALUES < ($minus_year, $minus_month, $minus_day, 12)"
  echo $queryDrop1
  echo $parDrop1
  stderr=$(impala-shell -V -i es-estimation-n5.internal.ads.dailyhunt.in -d default -q "$queryDrop1" 2>&1 1>${BASE_DIR}/kudu_partition_logs/logs )
  statusDrop=$?
  if [ $statusDrop -eq 0 ]
  then
    echo "IMPALA QUERY 4 SUCCESSFUL"
    partitionsDropped+=$parDrop1"<br>"
  else
    echo "IMPALA QUERY 4 UNSUCCESSFUL"
    error=$(echo $stderr | awk -F 'ERROR' '{ print $2 }')
    exception="ERROR"$error
    echo $exception
    partitionsNotDropped+=$parDrop1"<br>"$exception"<br>"
  fi

  queryDrop3="alter table default.$kudu_table_name drop range partition ($minus_year, $minus_month, $minus_day, 12) <= VALUES < ($minus_year2, $minus_month2, $minus_day2, 0)"
  parDrop3="PARTITION ($minus_year, $minus_month, $minus_day, 12) <= VALUES < ($minus_year2, $minus_month2, $minus_day2, 0)"
  echo $queryDrop3
  echo $parDrop3
  stderr=$(impala-shell -V -i es-estimation-n5.internal.ads.dailyhunt.in -d default -q "$queryDrop3" 2>&1 1>${BASE_DIR}/kudu_partition_logs/logs )
  statusDrop=$?
  if [ $statusDrop -eq 0 ]
  then
    echo "IMPALA QUERY 6 SUCCESSFUL"
    partitionsDropped+=$parDrop3"<br>"
  else
    echo "IMPALA QUERY 6 UNSUCCESSFUL"
    error=$(echo $stderr | awk -F 'ERROR' '{ print $2 }')
    exception="ERROR"$error
    echo $exception
    partitionsNotDropped+=$parDrop3"<br>"$exception"<br>"
  fi

else
  echo "#######ELSE##########"
  countsUnmatched="Percentage difference in Hive and Kudu counts for the day "$minus_year-$minus_month-$minus_day" is greater than 1%."
fi


subject="[Report]: Josh NRT data pipe: Kudu Range Partitions Dropped"
html_body="<html><body>KUDU RANGE PARTITIONS<br>"

if [ -z "$countsUnmatched" ]
then
  echo "countsUnmatched empty"
  if [ -z "$partitionsDropped" ]
  then
    echo "partitionsDropped empty"
  else
    html_body+="<br>Following range partitions have been dropped:<br>"
    html_body+="$partitionsDropped"
  fi

  if [ -z "$partitionsNotDropped" ]
  then
    echo "partitionsNotDropped empty"
  else
    html_body+="<br>Following range partitions could not be dropped:<br>"
    html_body+="$partitionsNotDropped"
  fi

else
  html_body+="<br>$countsUnmatched<br>"
fi

html_body+="</body></html>"
echo $html_body

#java -cp "${APPLICATION_JAR}" in.dailyhunt.utils.SendEmail "$subject" "$html_body"


