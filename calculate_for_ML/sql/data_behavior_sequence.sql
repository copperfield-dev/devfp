INSERT OVERWRITE TABLE data_behavior_sequence_7_to_7
SELECT abs.uid, abs.ds, seq
FROM ads_behavior_sequence_7_to_7 abs
         RIGHT JOIN tmp_player_7_to_7 tmp
                    ON abs.uid = tmp.uid
ORDER BY abs.uid, abs.ds;


INSERT OVERWRITE TABLE data_behavior_sequence_14_to_7
SELECT abs.uid, abs.ds, seq
FROM ads_behavior_sequence_14_to_7 abs
         RIGHT JOIN tmp_player_14_to_7 tmp
                    ON abs.uid = tmp.uid AND abs.ds = tmp.ds
ORDER BY abs.uid, abs.ds;


INSERT OVERWRITE TABLE data_behavior_sequence_14_to_14
SELECT abs.uid, abs.ds, seq
FROM ads_behavior_sequence_14_to_14 abs
         RIGHT JOIN tmp_player_14_to_14 tmp
                    ON abs.uid = tmp.uid AND abs.ds = tmp.ds
ORDER BY abs.uid, abs.ds;