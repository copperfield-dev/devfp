#!/bin/bash
start_date=$1
end_date=$2
host=$3

source /home/wuxin/miniconda3/bin/activate moyu-ml-python

date_str=$start_date
echo $date_str

while [[ "$date_str" != "$end_date" ]]
do
  echo $date_str

  echo "/bin/bash etl-log_extract.sh $date_str"
  /bin/bash ../etl/log_extract.sh $date_str

  cd /home/wuxin/english-moyu/data/$host/da
  mkdir $date_str
  mv *.log $date_str

  cd /home/wuxin/moyu-ml/tool
  echo "/bin/bash etl-log_transform.sh $date_str"
  /bin/bash ../etl/log_transform.sh $date_str

  cd /home/wuxin/moyu_ml/english-moyu
  mkdir $date_str
  mv *.log $date_str

  cd /home/wuxin/moyu-ml/tool
  echo "/bin/bash etl-log_load.sh $date_str"
  kinit -kt /home/zk/krb5keytab/ywmy.keytab ywmy
  /bin/bash ../etl/log_load.sh $date_str

  date_str=`date +%Y%m%d -d "$date_str +1day"`
done
