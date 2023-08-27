INSERT OVERWRITE TABLE data_social_network_7days_before
SELECT
    `user_id`,
    `friend_user_id`,
    `weight`,
    `host`
FROM
    dwd_user_social_snapshot
WHERE
    dt = date_sub('${hivevar:datestr}', 7)  AND host = '${hivevar:host}';


INSERT OVERWRITE TABLE data_social_network_14days_before
SELECT
    `user_id`,
    `friend_user_id`,
    `weight`,
    `host`
FROM
    dwd_user_social_snapshot
WHERE
    dt = date_sub('${hivevar:datestr}', 14)  AND host = '${hivevar:host}';