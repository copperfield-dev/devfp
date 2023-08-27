INSERT OVERWRITE TABLE tmp_social_player_7_to_7
SELECT social_player.uid FROM
    (
        SELECT user_id AS uid
        FROM data_social_network_7days_before
        UNION
        SELECT friend_user_id AS uid
        FROM data_social_network_7days_before
    ) social_player
        INNER JOIN
    (
        SELECT DISTINCT uid
        FROM tmp_player_7_to_7
    ) tmp_player
    ON social_player.uid = tmp_player.uid;

INSERT OVERWRITE TABLE tmp_social_player_14_to_7
SELECT social_player.uid FROM
    (
        SELECT user_id AS uid
        FROM data_social_network_7days_before
        UNION
        SELECT friend_user_id AS uid
        FROM data_social_network_7days_before
    ) social_player
        INNER JOIN
    (
        SELECT DISTINCT uid
        FROM tmp_player_14_to_7
    ) tmp_player
    ON social_player.uid = tmp_player.uid;

INSERT OVERWRITE TABLE tmp_social_player_14_to_14
SELECT social_player.uid FROM
    (
        SELECT user_id AS uid
        FROM data_social_network_14days_before
        UNION
        SELECT friend_user_id AS uid
        FROM data_social_network_14days_before
    ) social_player
        INNER JOIN
    (
        SELECT DISTINCT uid
        FROM tmp_player_14_to_14
    ) tmp_player
    ON social_player.uid = tmp_player.uid;