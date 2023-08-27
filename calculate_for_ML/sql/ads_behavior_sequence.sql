INSERT OVERWRITE TABLE ads_behavior_sequence_7_to_7
SELECT user_id AS uid, concat_ws(',', collect_set(cate_id)) as seq, host, dt AS ds
FROM
    (
        SELECT user_id, dt, cate_id, operation_num, host FROM
            (
                SELECT user_id, dt, cate_id, count(cate_id) as operation_num, host
                FROM
                    dwd_user_action
                WHERE
                    dt >= date_sub('${hivevar:datestr}', 14) AND dt <= date_sub('${hivevar:datestr}', 7) AND host = '${hivevar:host}'
                GROUP BY
                    user_id, dt, cate_id, host
            ) tmp
            DISTRIBUTE BY cate_id
  SORT BY user_id, operation_num DESC
    ) tmp1
GROUP BY user_id, dt, host;

INSERT OVERWRITE TABLE ads_behavior_sequence_14_to_7
SELECT user_id AS uid, concat_ws(',', collect_set(cate_id)) as seq, host, dt AS ds
FROM
    (
        SELECT user_id, dt, cate_id, operation_num, host FROM
            (
                SELECT user_id, dt, cate_id, count(cate_id) as operation_num, host
                FROM
                    dwd_user_action
                WHERE
                    dt >= date_sub('${hivevar:datestr}', 21) AND dt <= date_sub('${hivevar:datestr}', 7) AND host = '${hivevar:host}'
                GROUP BY
                    user_id, dt, cate_id, host
            ) tmp
            DISTRIBUTE BY cate_id
            SORT BY user_id, operation_num DESC
    ) tmp1
GROUP BY user_id, dt, host;

INSERT OVERWRITE TABLE ads_behavior_sequence_14_to_14
SELECT user_id AS uid, concat_ws(',', collect_set(cate_id)) as seq, host, dt AS ds
FROM
    (
        SELECT user_id, dt, cate_id, operation_num, host FROM
            (
                SELECT user_id, dt, cate_id, count(cate_id) as operation_num, host
                FROM
                    dwd_user_action
                WHERE
                    dt >= date_sub('${hivevar:datestr}', 28) AND dt <= date_sub('${hivevar:datestr}', 14) AND host = '${hivevar:host}'
                GROUP BY
                    user_id, dt, cate_id, host
            ) tmp
            DISTRIBUTE BY cate_id
            SORT BY user_id, operation_num DESC
    ) tmp1
GROUP BY user_id, dt, host;