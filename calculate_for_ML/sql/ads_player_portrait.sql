INSERT OVERWRITE TABLE ads_player_portrait_7_to_7
SELECT
  user_id AS uid, prof, level, vip, battle, face_id, hair_id, magic_stone_num, gold_num, host, dt AS ds
FROM dim_user_info
WHERE
  dt >= date_sub('${hivevar:datestr}', 14) AND dt <= date_sub('${hivevar:datestr}', 7) AND host = '${hivevar:host}'
ORDER BY
  uid, ds;


INSERT OVERWRITE TABLE ads_player_portrait_14_to_7
SELECT
  user_id AS uid, prof, level, vip, battle, face_id, hair_id, magic_stone_num, gold_num, host, dt AS ds
FROM dim_user_info
WHERE
  dt >= date_sub('${hivevar:datestr}', 21) AND dt <= date_sub('${hivevar:datestr}', 7) AND host = '${hivevar:host}'
ORDER BY
  uid, ds;


INSERT OVERWRITE TABLE ads_player_portrait_14_to_14
SELECT
  user_id AS uid, prof, level, vip, battle, face_id, hair_id, magic_stone_num, gold_num, host, dt AS ds
FROM dim_user_info
WHERE
  dt >= date_sub('${hivevar:datestr}', 28) AND dt >= date_sub('${hivevar:datestr}', 14) AND host = '${hivevar:host}'
ORDER BY
  uid, ds;