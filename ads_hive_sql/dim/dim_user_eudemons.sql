set spark.executor.instances=2;set spark.executor.memory=4g;set spark.executor.cores=2;
set spark.app.name=etl-dim_user_eudemons;
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.optimize.reducededuplication=false;

--因为我们是离线计算，所以每天只要处理当天的就好了。

-- 幻兽
with tmp_dim_user_eudemons as (
    select
        *,
        row_number() OVER(PARTITION BY host,dt,user_id,eudemons_id ORDER BY msg_time desc) AS rank
    from
        raw_user_eudemons
    where dt='$[yyyy-MM-dd(dd-1)]'
)

insert overwrite table dim_user_eudemons partition(dt='$[yyyy-MM-dd(dd-1)]',host)
select
    `user_id`,
    `eudemons_id`,
    `level`,
    `life`,
    `def`,
    `magic_def`,
    `msg_time`,
    `msg_time_ctz`,
    `host`
from
    tmp_dim_user_eudemons
where
    rank=1;