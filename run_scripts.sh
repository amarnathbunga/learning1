#!/bin/bash

BASE_DIR="/mnt/vol1/dh-ads/joshkuduhive"
APPLICATION_JAR=${BASE_DIR}/jar/Josh.jar
filename="${BASE_DIR}/scripts/days.properties"
line=$(head -n 1 $filename)
if [ -z $line ]
then
  echo "NUMBER OF DAYS EMPTY"
  subject="[Alert]: Josh NRT data pipe: Running scripts sequentially failed"
	html_body1="<html><body>Running scripts sequentially failed. Lower limit on number of days is not specified.</body></html>"
	java -cp "${APPLICATION_JAR}" in.dailyhunt.utils.SendEmail "$subject" "$html_body1"
  exit 1
fi

minus_year=$(date '+%Y' --date "-$line day")
minus_month=$(date '+%m' --date "-$line day")
minus_day=$(date '+%d' --date "-$line day")
echo $minus_year
echo $minus_month
echo $minus_day

#RUN KUDU HIVE SINKER
cd ${BASE_DIR}/scripts
sh ${BASE_DIR}/scripts/kudu_hive.sh
statusSinker=$?
echo "STATUS SINKER"
echo $statusSinker

if [ $statusSinker -eq 0 ]
then
  echo "KUDU HIVE SINKER SUCCESSFUL"
  echo "SLEEPING FOR 1 MINUTE"
  sleep 1m
  echo "SLEEP ENDED"
  #sh ${BASE_DIR}/scripts/impala_clicks_view.sh
  sh ${BASE_DIR}/scripts/impala_view.sh
  statusView=$?
  if [ $statusView -eq 0 ]
  then
    echo "IMPALA VIEW SUCCESSFUL"
    echo "SLEEPING FOR 1 MINUTE"
    sleep 1m
    echo "SLEEP ENDED"
    sh ${BASE_DIR}/scripts/kudu_partitions_count_check.sh
  else
    echo "IMPALA VIEW FAILED"
    subject="[Alert]: Josh NRT data pipe: Kudu Partitions Add/Drop for $minus_year-$minus_month-$minus_day did not start"
	  html_body="<html><body>Kudu Partitions Add/Drop for $minus_year-$minus_month-$minus_day did not start beacuse Impala Alter view failed.</body></html>"
    java -cp "${APPLICATION_JAR}" in.dailyhunt.utils.SendEmail "$subject" "$html_body"
  fi
else
  echo "KUDU HIVE SINKER FAILED"
  subject="[Alert]: Josh NRT data pipe: Impala Alter view and Kudu Partitions Add/Drop for $minus_year-$minus_month-$minus_day did not start"
	html_body="<html><body>Impala Alter view and Kudu Partitions Add/Drop for $minus_year-$minus_month-$minus_day did not start beacuse Kudu Hive Sinker failed.</body></html>"
  java -cp "${APPLICATION_JAR}" in.dailyhunt.utils.SendEmail "$subject" "$html_body"
fi


