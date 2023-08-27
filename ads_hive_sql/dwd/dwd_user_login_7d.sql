set spark.executor.instances=2;
set spark.executor.memory=4g;
set spark.executor.cores=2;
set spark.app.name=etl-dwd_user_login_7d;
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.optimize.reducededuplication=false;

-- 每天记录的是当天有登录的快照
-- 1、从raw_user_login里面找出昨天新增的用户登录的数据
-- 2、找出被波及到的数据的范围
-- 3、找出和登录数据无关的数据
-- 4、找和登录数据有关的数据,进行刷新
-- 5、对3和4进行union all,回写数据至各个分区,完成旧数据的刷新。
--


with tmp_user_login_last1d as (
    select
        user_id,
        lastlogin,
        is_login_within_seven_days,
        dt,
        host
    from
        (
            select
                user_id,
                concat('20', substr(lastlogin, 1, 6)) as lastlogin,
                null as is_login_within_seven_days,
                dt,
                host,
                row_number() OVER(
                    PARTITION BY host,
                        user_id
                    ORDER BY
                        msg_time desc
                    ) AS rank
            from
                raw_user_login
            where
                    dt = '$[yyyy-MM-dd(dd-1)]'
        ) a
    where
            a.rank = 1
),
--找出过去前8天内到前天的快照,因为要根据tmp_user_login把匹配到tmp_user_login记录的是否登录刷成1,其他保持不变
     tmp_user_login_within_last8d as(
         select
             *
         from
             dwd_user_login_7d
         where
                 dt >= date_add('$[yyyy-MM-dd(dd-1)]', -7)
           and dt <= date_add('$[yyyy-MM-dd(dd-1)]', -1)
     ),

     tmp_update_user_login_within_last8d as (
         select
             a.user_id as user_id,
             a.lastlogin as lastlogin,
             case when b.dt is null then -- 前7天无数据，当日份新增登录
                      case when a.dt = date_add('$[yyyy-MM-dd(dd-1)]', -7)
                          and a.is_login_within_seven_days is null then '0'
                           else a.is_login_within_seven_days
                          end
                  else '1'
                 end as is_login_within_seven_days,
             a.dt,
             a.host
         from
             tmp_user_login_within_last8d a
                 left join tmp_user_login_last1d b on a.user_id = b.user_id
                 and a.host = b.host
     ),
--将昨天和8天前到前天的数据进行合并。
     tmp_dwd_user_login_7d as (
         select
             user_id,
             lastlogin,
             is_login_within_seven_days,
             dt,
             host
         from
             tmp_user_login_last1d
         union all
         select
             user_id,
             lastlogin,
             is_login_within_seven_days,
             dt,
             host
         from
             tmp_update_user_login_within_last8d
     )

insert overwrite table dwd_user_login_7d partition(dt, host)
select
    user_id,
    lastlogin,
    is_login_within_seven_days,
    dt,
    host
from
    tmp_dwd_user_login_7d;