INSERT OVERWRITE TABLE data_player_portrait_7_to_7
SELECT app.uid AS uid, app.ds AS ds, prof, level, vip, battle, face_id, hair_id, magic_stone_num, gold_num, host
FROM ads_player_portrait_7_to_7 app
         RIGHT JOIN tmp_player_7_to_7 tmp
                    ON app.uid = tmp.uid
ORDER BY app.uid, app.ds;


INSERT OVERWRITE TABLE data_player_portrait_14_to_7
SELECT app.uid, app.ds, prof, level, vip, battle, face_id, hair_id, magic_stone_num, gold_num, host
FROM ads_player_portrait_14_to_7 app
         RIGHT JOIN tmp_player_14_to_7 tmp
                    ON app.uid = tmp.uid AND app.ds = tmp.ds
ORDER BY app.uid, app.ds;


INSERT OVERWRITE TABLE data_player_portrait_14_to_14
SELECT app.uid, app.ds, prof, level, vip, battle, face_id, hair_id, magic_stone_num, gold_num, host
FROM ads_player_portrait_14_to_14 app
         RIGHT JOIN tmp_player_14_to_14 tmp
                    ON app.uid = tmp.uid AND app.ds = tmp.ds
ORDER BY app.uid, app.ds;