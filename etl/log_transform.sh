#!/bin/bash

source ../script/english_moyu_function.sh
logdate=$1
current_date=${logdate}
moyu_date=$(get_moyu_log_date_str $current_date)
echo $moyu_date

for host in `cat ../resource/host_list.txt`
do
    python ../etl/log_transform_to_hdfs.py $moyu_date ${host}
    if [ $? != 0 ]
    then
        echo "python3 log_transform_to_hdfs.py $moyu_date ${host} failed"
        exit 1
    fi
done
