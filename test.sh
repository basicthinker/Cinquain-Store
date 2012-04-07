#!/bin/bash

#Benchmark test for Cinquain-Store

#As Cinquain-Store is build over redis servers, memory will limit the performance.Redis forks sub process for backup, so twice mount memory is needed as  allocation to redis server.

#For more tests of 
#*remote servers(can config in redis_server.config)
#*in more servers with less memory(such as 6 servers with 1 GB memory)
#*in less servers with more memory(such as 3 servers with 2 GB memory)
#(can config in redis.conf)

#Keep this file in the same directory with the store_test program.
#execute like this : ./test.sh testfilename times , test result will be saved in XXXXX(executing time).testfilename.times.log

#what's this benchmark test?

#use many clients (1, 5, 10 processes)

#*writerange & readrange many(500) small file(KB level)
#*writerange & readrange some huge file(GB level)
#specify with the filename & times params 

#with a specific range(4 KB, 32 KB, 256 KB, 1 MB)

#record & figure out throughput in MB/s

declare -a range
declare -a clients_num

range[0]=4
range[1]=32
range[2]=64
range[3]=256
clients_num[0]=1
clients_num[1]=10
clients_num[2]=100

#save log for current executing
log=$(date '+%Y-%m-%d_%H:%M:%S').$1.$2.log

line=0
cline=0

for i in ${range[@]}; do
    for j in ${clients_num[@]}; do
        echo $j clients range in $i KB, each client W/R throughput list in MB/s:>>$log
            line=$((line+1))
            for((k=0; k<j; k++))
            do
                ./store_test $1 $i $2>>$log &
            done
            line=$((line+j))
            cline=`cat $log |wc -l`
            while [ $line -ne $cline ]; do
                sleep 10
                cline=`cat $log |wc -l`
            done
    done
done
