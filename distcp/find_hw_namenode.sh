curl -i -u dbpp:dbpp -H "X-Requested-By: ambari" -X GET 'http://dh-hw-m1d41.dpp.dailyhunt.in:8080/api/v1/clusters/hwcluster01/host_components?HostRoles/component_name=NAMENODE&metrics/dfs/FSNamesystem/HAState=active' > /tmp/namenode_detect.txt

namenode=`grep "host_name"  /tmp/namenode_detect.txt| awk '{print $3}' | sed 's/\"//g'`

echo "namenode is $namenode"

export HW_NAMENODE_IP=$namenode
