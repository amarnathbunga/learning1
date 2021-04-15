#ls -ltrh
utcTime=$1
year=$(echo $(date -d $utcTime +'%Y-%m-%d %H') | cut -d - -f 1)
month=$(echo $(date -d $utcTime +'%Y-%m-%d %H') | cut -d - -f 2 |sed 's/^0*//')
day=$(echo $(date -d $utcTime +'%Y-%m-%d %H') | cut -d - -f 3 | cut -d ' ' -f 1 |sed 's/^0*//')
hour=$(echo $(date -d $utcTime +'%Y-%m-%d %H') | cut -d - -f 3 | cut -d ' ' -f 2 |sed 's/^0*//')

echo $year
echo $month
echo $day
echo $hour
