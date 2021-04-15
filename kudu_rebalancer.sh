#!/bin/bash
echo "Kudu REBALANCER STARTED"
sudo -u kudu kudu cluster rebalance es-estimation-n3.internal.ads.dailyhunt.in:7051,es-estimation-n2.internal.ads.dailyhunt.in:7051,es-estimation-n1.internal.ads.dailyhunt.in:7051
statusbalancer=$?
echo "SPARK STATUS"
echo $statusbalancer
if [ $statusbalancer -eq 0 ]
then
    echo "KUDU REBALANCER SUCCESSFULL"
    subject="ALERT : KUDU REBALANCER SUCCESSFULL"
    html_body="<html><body>kudu rebalancer has failed.</body></html>"
    java -cp "/mnt/vol1/dh-ads/kuduhive/jar/NrtDataProcessing-kudu_one_replica_wp-assembly-0.1.jar"  in.dailyhunt.utils.SendEmail "$subject" "$html_body"
else
    echo "KUDU REBALANCER FAILED"
    subject="ALERT : KUDU REBALANCER FAILED"
    html_body="<html><body>kudu rebalancer has failed.</body></html>"
    java -cp "/mnt/vol1/dh-ads/kuduhive/jar/NrtDataProcessing-kudu_one_replica_wp-assembly-0.1.jar"  in.dailyhunt.utils.SendEmail "$subject" "$html_body"
fi
