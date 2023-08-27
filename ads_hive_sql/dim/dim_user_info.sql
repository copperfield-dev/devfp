set spark.executor.instances=2;
set spark.executor.memory=4g;
set spark.executor.cores=2;
set spark.app.name=etl-dim_user_info;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.optimize.reducededuplication=false;

--因为我们是离线计算，所以每天只要处理当天的就好了。
WITH tmp_dim_user_info AS (
    SELECT *
    FROM (
             SELECT ul.*,
                    ROW_NUMBER() OVER (PARTITION BY host,dt,user_id,account_id ORDER BY msg_time desc) AS ranknum
             FROM raw_user_login AS ul
         ) AS ul
    WHERE ul.ranknum = 1
      AND ul.dt = '$[yyyy-MM-dd(dd-1)]'
),
 tmp_dim_user_extension as (
     SELECT *
     FROM (
              SELECT ue.*,
                     ROW_NUMBER() OVER (PARTITION BY host,dt,user_id,face_id ORDER BY msg_time desc) AS ranknum
              FROM raw_user_extension AS ue
          ) AS ue
     WHERE ue.ranknum = 1
       AND ue.dt = '$[yyyy-MM-dd(dd-1)]'
)

INSERT OVERWRITE TABLE dim_user_info partition(dt='$[yyyy-MM-dd(dd-1)]', host)
SELECT
    ul.`user_id`,
    `account_id`,
    `prof`,
    `level`,
    `vip`,
    `battle`,
    ul.`msg_time`,
    ul.`msg_time_ctz`,
    `face_id`,
    `hair_id`,
    `magic_stone_num`,
    `gold_num`
FROM
    tmp_dim_user_info AS ul
INNER JOIN tmp_dim_user_extension AS ue
ON ul.user_id = ue.user_id AND ul.dt = ue.dt