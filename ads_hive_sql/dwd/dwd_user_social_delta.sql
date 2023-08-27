set spark.executor.instances=2;set spark.executor.memory=4g;set spark.executor.cores=2;
set spark.app.name=etl-dwd_user_social_delta;
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.optimize.reducededuplication=false;


--每日增量的好友关系
with tmp_friend_relationship  as (
    select * from (
                      select
                          * ,
                          row_number() OVER(PARTITION BY host,user_id,friend_user_id ORDER BY msg_time desc) AS rank
                      from
                          raw_friend_relationship
                      where
                              dt='$[yyyy-MM-dd(dd-1)]'
                  ) a
    where a.rank=1

),
-- 截止到那天为止，尽可能新的用户信息
     tmp_user_family as (
         select * from
             (

                 select
                     * ,
                     row_number() OVER(PARTITION BY host,user_id ORDER BY msg_time desc) AS rank
                 from
                     raw_user_family
                 where
                         dt='$[yyyy-MM-dd(dd-1)]'
             ) a
         where a.rank=1
     ),
--通过用户关系表和用户家族关系表来找出用户好友之间是否是同个家族，同个帮派，情侣关系
     tmp_user_social as (
         select
             a.user_id as user_id,
             a.friend_user_id as friend_user_id,
             a.msg_time as msg_time,
             b.syn_id as user_syn_id,
             b.mate_id as user_mate_id,
             b.family_id as user_family_id,
             c.syn_id as friend_syn_id,
             c.mate_id as friend_mate_id,
             c.family_id as friend_family_id,
             if(COALESCE(b.syn_id,-1)=COALESCE(c.syn_id,-2) and COALESCE(b.syn_id,-1)!=0,1,0) as syn_weight,
             if(COALESCE(b.mate_id,-1)=COALESCE(a.friend_user_id,-2) and COALESCE(b.mate_id,-1)!=0 ,1,0) as mate_weight,
             if(COALESCE(b.family_id,-1)=COALESCE(c.family_id,-2) and COALESCE(b.family_id,-1)!=0 ,1,0) as family_weight,
             a.host as host,
             a.dt as dt
         from
             tmp_friend_relationship a
                 left join tmp_user_family b on a.user_id= b.user_id and a.host = b.host
                 left join tmp_user_family c on a.friend_user_id = c.user_id and a.host = c.host
     )

insert overwrite table dwd_user_social_delta partition(dt='$[yyyy-MM-dd(dd-1)]',host)
select
    `user_id`,
    `friend_user_id`,
    1+syn_weight+mate_weight+family_weight as  `weight`,
    `msg_time`,
    `host`
from
    tmp_user_social;