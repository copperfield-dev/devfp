#!/bin/bash
start_date=`date +%Y%m%d --date="-1 day"`
end_date=`date +%Y%m%d`

echo ${start_date}
echo ${end_date}

cd /home/wuxin/moyu-ml/tool/ 
kinit -kt /home/zk/krb5keytab/ywmy.keytab ywmy
/bin/bash -x complete_num.sh ${start_date} ${end_date} 47.253.49.46
