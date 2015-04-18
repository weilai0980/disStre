#!/bin/bash

cd /root/guo/stormCluster/zookeeper-3.4.5/

java -cp zookeeper-3.4.5.jar:/root/guo/stormCluster/zookeeper-3.4.5/lib/slf4j-api-1.6.1.jar:conf org.apache.zookeeper.server.PurgeTxnLog /var/zookeeper/ /var/zookeeper -n 3

cd /root/guo/storm-0.8.2/
rm -r logs

#cd /var/zookeeper
#rm -r version-2 
