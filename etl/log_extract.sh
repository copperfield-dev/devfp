#!/bin/bash

source /home/wuxin/miniconda3/bin/activate moyu-ml-python
source ../script/english_moyu_function.sh
logdate=$1
filename_suffix=$(get_moyu_log_date_str $logdate)
filename[0]="msgaction "
filename[1]="msgaction2 "
filename[2]="msgtype "
filename[3]="userinfo "
current_date=${logdate}
moyu_date=$(get_moyu_log_date_str $current_date)
echo $moyu_date

for host in `cat ../resource/host_list.txt`
do
    echo "host is: $host"
    for ((i = 0; i < ${#filename[*]}; i++))
    do
        echo "python3 ../tool/ftp_download.py -c ${host}.json -f \"da/${filename[i]}$moyu_date.log\""
        python ../tool/ftp_download.py -c ../resource/${host}.json -f "da/${filename[i]}$moyu_date.log"
        if [ $? != 0 ]
        then
            echo "python3 ../tool/ftp_download.py -c ${host}.json -f  \"da/${filename[i]}$moyu_date.log\" failed"
        	exit 1
        fi
    done
done
