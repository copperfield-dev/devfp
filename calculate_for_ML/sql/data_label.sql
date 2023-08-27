INSERT OVERWRITE TABLE data_label_7_to_7
SELECT tmp.uid AS uid, churn_label
FROM
    (
        SELECT
            dul7d.user_id AS uid,
            dul7d.dt,
            dul7d.is_login_within_seven_days AS churn_label,
            ROW_NUMBER() OVER (PARTITION BY dul7d.user_id ORDER BY dul7d.user_id, dul7d.dt DESC) AS rownum
        FROM dwd_user_login_7d AS dul7d
        WHERE dul7d.dt >= date_sub('${hivevar:datestr}', 7)
          AND dul7d.dt <= '${hivevar:datestr}'
    ) AS tmp1
        RIGHT JOIN tmp_player_7_to_7 AS tmp
                   ON tmp1.uid = tmp.uid
WHERE rownum = 1
GROUP BY tmp.uid, churn_label
ORDER BY uid;


INSERT OVERWRITE TABLE data_label_14_to_7
SELECT tmp.uid AS uid, churn_label
FROM
    (
        SELECT
            dul7d.user_id AS uid,
            dul7d.dt,
            dul7d.is_login_within_seven_days AS churn_label,
            ROW_NUMBER() OVER (PARTITION BY dul7d.user_id ORDER BY dul7d.user_id, dul7d.dt DESC) AS rownum
        FROM dwd_user_login_7d AS dul7d
        WHERE dul7d.dt >= date_sub('${hivevar:datestr}', 7)
          AND dul7d.dt <= '${hivevar:datestr}'
    ) AS tmp1
        RIGHT JOIN tmp_player_14_to_7 AS tmp
                   ON tmp1.uid = tmp.uid
WHERE rownum = 1
GROUP BY tmp.uid, churn_label
ORDER BY uid;


INSERT OVERWRITE TABLE data_label_14_to_14
SELECT tmp.uid AS uid, churn_label
FROM
    (
        SELECT
            dul7d.user_id AS uid,
            dul7d.dt,
            dul7d.is_login_within_seven_days AS churn_label,
            ROW_NUMBER() OVER (PARTITION BY dul7d.user_id ORDER BY dul7d.user_id, dul7d.dt DESC) AS rownum
        FROM dwd_user_login_14d AS dul14d
        WHERE dul7d.dt >= date_sub('${hivevar:datestr}', 14)
          AND dul7d.dt <= '${hivevar:datestr}'
    ) AS tmp1
        RIGHT JOIN tmp_player_14_to_14 AS tmp
                   ON tmp1.uid = tmp.uid
WHERE rownum = 1
GROUP BY tmp.uid, churn_label
ORDER BY uid;