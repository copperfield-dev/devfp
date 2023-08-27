#!/bin/bash

source ../script/english_moyu_function.sh
logdate=$1
current_date=${logdate}
moyu_date=$(get_moyu_log_date_str $current_date)
echo $moyu_date

table[0]="user_login"
table[1]="user_family"
table[2]="user_eudemons"
table[3]="user_equipment"
table[4]="friend_relationship"
table[5]="enemy_relationship"
table[6]="user_extension"
table[7]="msgaction"
table[8]="msgaction2"
table[9]="msgtype"

function load_data_to_hdfs() {
    host=$1
    table=$2
    if [ -f "/home/wuxin/moyu_ml/english-moyu/${logdate}/${table}_${host}.${moyu_date}.log" ]
    then
        /home/hadoop/hadoop/bin/hadoop fs -mkdir -p /user/nd_rdg/raw-data/offline/english_moyu/raw_${table}/${host}/${logdate}/
        /home/hadoop/hadoop/bin/hadoop fs -rm /user/nd_rdg/raw-data/offline/english_moyu/raw_${table}/${host}/${logdate}/${table}_${host}.${moyu_date}.log
        /home/hadoop/hadoop/bin/hadoop fs -put /home/wuxin/moyu_ml/english-moyu/${logdate}/${table}_${host}.${moyu_date}.log /user/nd_rdg/raw-data/offline/english_moyu/raw_${table}/${host}/${logdate}/
    fi
}

for host in `cat ../resource/host_list.txt`
do
    for (( i = 0; i < ${#table[*]}; i++ )); do
        load_data_to_hdfs ${host} ${table[i]}
    done
done
