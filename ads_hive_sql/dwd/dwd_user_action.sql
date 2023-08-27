set spark.executor.instances = 2;
set spark.executor.memory = 4g;
set spark.executor.cores = 2;
set spark.app.name = etl - dwd_user_action;
set hive.exec.dynamici.partition = true;
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.max.dynamic.partitions = 10000;
set hive.exec.max.dynamic.partitions.pernode = 1000;
set hive.optimize.reducededuplication = false;
with tmp_msg_type_1 as (
    select line_num,
        user_id,
        cate_id,
        '0' as action_id,
        msg_time,
        dt,
        host
    from raw_msg_type
    where dt = '$[yyyy-MM-dd(dd-1)]'
        and cate_id != '1010'
        and cate_id != '1032'
),
tmp_msg_type_2 as (
    select line_num,
        user_id,
        cate_id,
        '0' as action_id,
        msg_time,
        dt,
        host,
        row_number() OVER(
            PARTITION BY host,
            dt,
            user_id
            ORDER BY msg_time desc,
                line_num desc
        ) AS rank
    from raw_msg_type
    where dt = '$[yyyy-MM-dd(dd-1)]'
        and cate_id = '1010'
),
tmp_msg_type_3 as (
    select line_num,
        user_id,
        cate_id,
        '0' as action_id,
        msg_time,
        dt,
        host,
        row_number() OVER(
            PARTITION BY host,
            dt,
            user_id
            ORDER BY msg_time desc,
                line_num desc
        ) AS rank
    from raw_msg_type
    where dt = '$[yyyy-MM-dd(dd-1)]'
        and cate_id = '1032'
),
tmp_action as (
    select line_num,
        user_id,
        cate_id,
        action_id,
        msg_time,
        dt,
        host,
        row_number() OVER(
            PARTITION BY host,
            dt,
            user_id
            ORDER BY msg_time desc,
                line_num desc
        ) AS rank
    from raw_msg_action
    where dt = '$[yyyy-MM-dd(dd-1)]'
),
tmp_action2 as (
    select line_num,
        user_id,
        cate_id,
        action_id,
        msg_time,
        dt,
        host,
        row_number() OVER(
            PARTITION BY host,
            dt,
            user_id
            ORDER BY msg_time desc,
                line_num desc
        ) AS rank
    from raw_msg_action2
    where dt = '$[yyyy-MM-dd(dd-1)]'
),
tmp_msg_type_combine_action as (
    select a.line_num as line_num,
        a.user_id as user_id,
        a.cate_id as cate_id,
        COALESCE(b.action_id, a.action_id) as action_id,
        a.msg_time as msg_time,
        a.dt as dt,
        a.host as host
    from tmp_msg_type_2 a
        left join tmp_action b on a.user_id = b.user_id
        and a.dt = b.dt
        and a.host = b.host
        and a.rank = b.rank
),
tmp_msg_type_combine_action2 as (
    select a.line_num as line_num,
        a.user_id as user_id,
        a.cate_id as cate_id,
        COALESCE(b.action_id, a.action_id) as action_id,
        a.msg_time as msg_time,
        a.dt as dt,
        a.host as host
    from tmp_msg_type_3 a
        left join tmp_action2 b on a.user_id = b.user_id
        and a.dt = b.dt
        and a.host = b.host
        and a.rank = b.rank
),
tmp_action_seq_record as (
    select *
    from tmp_msg_type_1
    union all
    select *
    from tmp_msg_type_combine_action
    union all
    select *
    from tmp_msg_type_combine_action2
),
tmp_action_seq_str as (
    select line_num,
        user_id,
        cate_id,
        action_id,
        cast(
            cast(cate_id as int) * 10000 + cast(action_id as int) as string
        ) as combine_action_id,
        msg_time,
        dt,
        host
    FROM tmp_action_seq_record
)
insert overwrite table dwd_user_action partition(dt = '$[yyyy-MM-dd(dd-1)]', host)
select `line_num`,
    `user_id`,
    `cate_id`,
    `action_id`,
    `combine_action_id`,
    `msg_time`,
    `host`
from tmp_action_seq_str;