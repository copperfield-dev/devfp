INSERT OVERWRITE TABLE tmp_player_7_to_7
SELECT DISTINCT app.uid
FROM
    ads_player_portrait_7_to_7 app
        INNER JOIN
    (
        SELECT * FROM ads_behavior_sequence_7_to_7
    ) abs ON app.uid = abs.uid
        INNER JOIN
    (
        SELECT * FROM dwd_user_login_7d
        WHERE is_login_within_seven_days IS NOT NULL
          AND dt >= date_sub('${hivevar:datestr}', 7)
          AND dt <= '${hivevar:datestr}'
    ) dul7d on app.uid = dul7d.user_id
ORDER BY app.uid;


INSERT OVERWRITE TABLE tmp_player_14_to_7
SELECT DISTINCT app.uid
FROM
    ads_player_portrait_14_to_7 app
        INNER JOIN
    (
        SELECT * FROM ads_behavior_sequence_14_to_7
    ) abs ON app.uid = abs.uid
        INNER JOIN
    (
        SELECT * FROM dwd_user_login_7d
        WHERE is_login_within_seven_days IS NOT NULL
          AND dt >= date_sub('${hivevar:datestr}', 7)
          AND dt <= '${hivevar:datestr}'
    ) dul7d on app.uid = dul7d.user_id
ORDER BY app.uid;


INSERT OVERWRITE TABLE tmp_player_14_to_14
SELECT DISTINCT app.uid
FROM
    ads_player_portrait_14_to_14 app
        INNER JOIN
    (
        SELECT * FROM ads_behavior_sequence_14_to_14
    ) abs ON app.uid = abs.uid
        INNER JOIN
    (
        SELECT * FROM dwd_user_login_14d
        WHERE is_login_within_seven_days IS NOT NULL
          AND dt >= date_sub('${hivevar:datestr}', 14)
          AND dt <= '${hivevar:datestr}'
    ) dul14d on app.uid = dul14d.user_id
ORDER BY app.uid;