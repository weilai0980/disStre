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


pssh -h allhost.txt  -i svc -x /root/guo/service
pssh -h allhost.txt -i svc -k /root/guo/service

pssh -h allhost.txt -i rm -r /root/guo/service/zookeeper.out
pssh -h allhost.txt -i rm -r /root/guo/service/supervise 





