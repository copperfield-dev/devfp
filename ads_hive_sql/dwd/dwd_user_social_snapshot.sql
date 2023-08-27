set spark.executor.instances=2;set spark.executor.memory=4g;set spark.executor.cores=2;
set spark.app.name=etl-dwd_user_social_snapshot;
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.optimize.reducededuplication=false;

--前天的全量快照+昨天的delta，形成昨天的快照
--1、对增量进行处理，生成对称的
--2、全量中合并增量中没有的，这样生成一个全量的好友关系表
--3、根据最新的特点的数据，然后关联计算出最新的全量表
--每日增量的好友关系

--获取昨天增量的
with tmp_dwd_user_social_delta_1  as (
    select
        user_id,
        friend_user_id,
        host
    from
        dwd_user_social_delta
    where
            dt='$[yyyy-MM-dd(dd-1)]'
),
-- 好友关系对称
     tmp_dwd_user_social_delta_2 as (
         select
             friend_user_id as user_id ,
             user_id as friend_user_id,
             host
         from
             dwd_user_social_delta
         where
                 dt='$[yyyy-MM-dd(dd-1)]'
     ),

-- 获取前日全量的
     tmp_dwd_user_social_snapshot_d2 as (
         select
             user_id,
             friend_user_id,
             host
         from
             dwd_user_social_snapshot
         where
                 dt='$[yyyy-MM-dd(dd-2)]'
     ),

     tmp_dwd_user_social as (
         select
             *
         from
             tmp_dwd_user_social_delta_1
         union
         select
             *
         from
             tmp_dwd_user_social_delta_2
         union
         select
             *
         from
             tmp_dwd_user_social_snapshot_d2
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
-- 将昨日全量的好友关系和用户帮派属性信息进行关联，计算出当前最新的权重快照
     tmp_dwd_user_social_snapshot as (
         select
             a.user_id as user_id,
             a.friend_user_id as friend_user_id,
             b.syn_id as user_syn_id,
             b.mate_id as user_mate_id,
             b.family_id as user_family_id,
             c.syn_id as friend_syn_id,
             c.mate_id as friend_mate_id,
             c.family_id as friend_family_id,
             if(COALESCE(b.syn_id,-1)=COALESCE(c.syn_id,-2) and COALESCE(b.syn_id,-1)!=0,1,0) as syn_weight,
             if(COALESCE(b.mate_id,-1)=COALESCE(a.friend_user_id,-2) and COALESCE(b.mate_id,-1)!=0 ,1,0) as mate_weight,
             if(COALESCE(b.family_id,-1)=COALESCE(c.family_id,-2) and COALESCE(b.family_id,-1)!=0 ,1,0) as family_weight,
             a.host as host
         from
             tmp_dwd_user_social a
                 left join tmp_user_family b on a.user_id= b.user_id and a.host = b.host
                 left join tmp_user_family c on a.friend_user_id = c.user_id and a.host = c.host
     )

insert overwrite table dwd_user_social_snapshot partition(dt='$[yyyy-MM-dd(dd-1)]',host)
select
    `user_id`,
    `friend_user_id`,
    1+syn_weight+mate_weight+family_weight as `weight`,
    `host`
from
    tmp_dwd_user_social_snapshot;
