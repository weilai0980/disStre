#!/bin/bash

#PATH1='/home/guo/workspace/tsdb_cloud/hbase/hbase-0.92.1.jar'
#CLASSPATH=$PATH1:"$CLASSPATH"
#PATH2='/home/guo/workspace/tsdb_cloud/hbase/hbase-0.92.1-tests.jar'
#CLASSPATH=$PATH2:"$CLASSPATH"

#for i in /home/guo/workspace/tsdb_cloud/hbase/lib/*.jar; 
#do CLASSPATH=/$i:"$CLASSPATH"; 
#done 

#for i in /home/guo/workspace/tsdb_cloud/hbase/conf/*.*; 
#do CLASSPATH=/$i:"$CLASSPATH"; 
#done 

#for i in /home/guo/workspace/tsdb_cloud/hadoop/*.jar; 
#do CLASSPATH=/$i:"$CLASSPATH"; 
#done 

#for i in /home/guo/workspace/tsdb_cloud/hadoop/hadoop-conf/*.*; 
#do CLASSPATH=/$i:"$CLASSPATH"; 
#done 


scp -r /home/guo/disStre/robStre/robstre/robStream/src/ root@lsir-cluster-01.epfl.ch:/root/guo/aps

#scp -r /home/guo/disStre/conqry/streamqry/src root@lsir-cluster-01.epfl.ch:/root/guo/aps

#pssh -h allhost.txt -i rm -r /root/guo/storm-local/ 
pssh -h allhost.txt -i rm -r /var/zookeeper/version-2/

pssh -h allhost.txt -i bash /root/guo/stormCluster/zkClr.sh
pssh -h allhost.txt -i bash /root/guo/stormCluster/stormClr.sh
