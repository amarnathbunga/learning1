#!/bin/bash

BASE_DIR="/mnt/vol1/dh-ads/joshkuduhive"
APPLICATION_JAR=${BASE_DIR}/jar/Josh.jar

PYTHON_BIN="/mnt/vol1/python3.6/bin/python3.6"


alter_query_generator_script=${BASE_DIR}/scripts/alter_view_query_generate.py

kudu_table_name="josh_nrt_ads_events_data"
hive_table_name="josh_nrt_ads_events"
HIVE_VIEW="josh_nrt_ads_events_view"


filename=${BASE_DIR}/scripts/days.properties

line=$(head -n 1 $filename)
line=8
if [ -z $line ]
then
  echo "NUMBER OF DAYS EMPTY";
  subject="[Alert]: Josh NRT data pipe: Impala Alter view failed";
  html_body1="<html><body>Impala Alter view failed. Lower limit on number of days is not specified.</body></html>";
  java -cp ${APPLICATION_JAR} in.dailyhunt.utils.SendEmail "$subject" "$html_body1"
  exit 1
fi

current_year=$(date '+%Y' --date "-$line day")
current_month=$(date '+%m' --date "-$line day")
current_day=$(date '+%d' --date "-$line day")

current_year_minus5=$(date '+%Y' --date "-5 day")
current_year_minus15=$(date '+%Y' --date "-15 day")
current_month_minus15=$(date '+%m' --date "-15 day")
current_day_minus15=$(date '+%d' --date "-15 day")

current_year_plus=$(date '+%Y')
current_month_plus=$(date '+%m')
current_day_plus=$(date '+%d')

echo $current_year-$current_month-$current_day
echo $current_year_minus15-$current_month_minus15-$current_day_minus15
echo $current_year_plus-$current_month_plus-$current_day_plus

# TODO: Maybe include has_ad_interactions = 1 conditions in count
#CHECK IF HIVE COUNT MATCHES KUDU COUNT
impala-shell -V -i es-estimation-n5.internal.ads.dailyhunt.in -d default -q "refresh $hive_table_name"
hive_count=$(impala-shell -V -i es-estimation-n5.internal.ads.dailyhunt.in -d default -q "select count(*) from $hive_table_name where event_year=$current_year and event_month=$current_month and event_day=$current_day")
kudu_count=$(impala-shell -V -i es-estimation-n5.internal.ads.dailyhunt.in -d default -q "select count(*) from $kudu_table_name where event_year=$current_year and event_month=$current_month and event_day=$current_day and (has_impression = 1 or has_click = 1 or has_install = 1 or has_lead = 1 or has_postinstall = 1 or has_vast = 1)")

harray=( $( for i in $hive_count ; do echo $i ; done ) )
hcnt=${#harray[@]}
echo "##########HIVE COUNT#######"
echo "${harray[6]}"

array=( $( for i in $kudu_count ; do echo $i ; done ) )
cnt=${#array[@]}
echo "##########KUDU COUNT#######"
echo "${array[6]}"

if [ $("${harray[6]} == 0" | bc) = 0 ]
then
	count_percent=$(echo "(${array[6]}-${harray[6]})*100/${harray[6]}" | bc -l )
else
	count_percent=0
fi
if [ $count_percent < 1 ]
then
  echo "COUNTS MATCHED"
  echo "ALTER IMPALA VIEW"

  if [ $current_year -eq $current_year_minus15 ]
  then
    if [ $current_month -eq $current_month_minus15 ]
    then
      if [ $current_year -eq $current_year_plus ]
      then
        if [ $current_month -eq $current_month_plus ]
        then
          kudu_table_selection_criteria="event_year= $current_year and event_month=$current_month and event_day>$current_day"
          hive_table_selection_criteria="event_year=$current_year and event_month=$current_month_minus15 and event_day>$current_day_minus15"
        else
          kudu_table_selection_criteria="event_year= $current_year and ((event_month=$current_month and event_day>$current_day) or (event_month=$current_month_plus and event_day>=1))"
          hive_table_selection_criteria="event_year=$current_year and event_month=$current_month_minus15 and event_day>$current_day_minus15"
        fi
      else
        kudu_table_selection_criteria="(event_year= $current_year and event_month=$current_month and event_day>$current_day) or (event_year = $current_year_plus and event_month=$current_month_plus and event_day>=1)"
        hive_table_selection_criteria="event_year=$current_year and event_month=$current_month_minus15 and event_day>$current_day_minus15"
      fi
    else
      if [ $current_year -eq $current_year_plus ]
      then
        if [ $current_month -eq $current_month_plus ]
        then
          kudu_table_selection_criteria="event_year= $current_year and event_month=$current_month and event_day>$current_day"
          hive_table_selection_criteria="event_year=$current_year and ((event_month=$current_month_minus15 and event_day>$current_day_minus15) or (event_month=$current_month and event_day>=1))"
        else
          kudu_table_selection_criteria="event_year= $current_year and ((event_month=$current_month and event_day>$current_day) or (event_month=$current_month_plus and event_day>=1))"
          hive_table_selection_criteria="event_year=$current_year and ((event_month=$current_month_minus15 and event_day>$current_day_minus15) or (event_month=$current_month and event_day>=1))"
        fi
      else
        kudu_table_selection_criteria="(event_year= $current_year and event_month=$current_month and event_day>$current_day) or (event_year = $current_year_plus and event_month=$current_month_plus and event_day>=1)"
        hive_table_selection_criteria="event_year=$current_year and ((event_month=$current_month_minus15 and event_day>$current_day_minus15) or (event_month=$current_month and event_day>=1))"
      fi
    fi
  else
    if [ $current_year -eq $current_year_plus ]
    then
      if [ $current_month -eq $current_month_plus ]
      then
        kudu_table_selection_criteria="event_year= $current_year and event_month=$current_month and event_day>$current_day"
        hive_table_selection_criteria="(event_year=$current_year and event_month=$current_month and event_day>=1)  or (event_year=$current_year_minus15 and event_month=$current_month_minus15 and event_day>$current_day_minus15)"
      else
        kudu_table_selection_criteria="event_year= $current_year and ((event_month=$current_month and event_day>$current_day) or (event_month=$current_month_plus and event_day>=1))"
        hive_table_selection_criteria="(event_year=$current_year and event_month=$current_month and event_day>=1)  or (event_year=$current_year_minus15 and event_month=$current_month_minus15 and event_day>$current_day_minus15)"
      fi
    else
      kudu_table_selection_criteria="(event_year= $current_year and event_month=$current_month and event_day>$current_day) or (event_year = $current_year_plus and event_month=$current_month_plus and event_day>=1)"
      hive_table_selection_criteria="(event_year=$current_year and event_month=$current_month and event_day>=1)  or (event_year=$current_year_minus15 and event_month=$current_month_minus15 and event_day>$current_day_minus15)"
    fi
  fi
  
  echo "Generating the alter query using ${alter_query_generator_script}"

  queryAlterView=$(${PYTHON_BIN} ${alter_query_generator_script} $kudu_table_name $hive_table_name $HIVE_VIEW "$kudu_table_selection_criteria" "$hive_table_selection_criteria")

  echo "$queryAlterView"

  stderr=$(impala-shell -V -i es-estimation-n5.internal.ads.dailyhunt.in -d default -q "$queryAlterView" 2>&1 1>${BASE_DIR}/impala_view_logs/logs )
  statusView=$?
  if [ $statusView -eq 0 ]
  then
    echo "IMPALA QUERY SUCCESSFUL"
    subject="[Report]: Josh NRT data pipe: Impala Alter view for $current_year-$current_month-$current_day successful"
    html_body="<html><body>Josh Impala alter view for the day "$current_year-$current_month-$current_day" is done.</body></html>"
    java -cp "${APPLICATION_JAR}" in.dailyhunt.utils.SendEmail "$subject" "$html_body"
  else
    echo "IMPALA QUERY UNSUCCESSFUL"
#    echo "$stderr"
    error=$(echo $stderr | awk -F 'ERROR' '{ print $2 }')
    exception="ERROR"$error
    echo $exception
    subject="[Alert] : Josh NRT data pipe: Impala Alter view for $current_year-$current_month-$current_day failed"
    html_body="<html><body>Impala alter view for the day "$current_year-$current_month-$current_day" failed with the following exception:<br>$exception</body></html>"
    java -cp "${APPLICATION_JAR}" in.dailyhunt.utils.SendEmail "$subject" "$html_body"
    exit 1
  fi

else
  echo "COUNTS DID NOT MATCH"
  sh ${BASE_DIR}/scripts/hours_count_check.sh
  #echo "COUNTS DID NOT MATCH"
  #send email here
  #subject="ACTION NEEDED: Kudu Hive difference is $count_percent $current_year-$current_month-$current_day"
  html_body="<html><body> Impala alter view failed for "$current_year-$current_month-$current_day" </body></html>"
  #java -cp "${BASE_DIR}/jar/JoshNrtDataProcessing.jar" in.dailyhunt.utils.SendEmail "$subject" "$html_body"
  exit 1
fi



