ALTER TABLE raw_enemy_relationship ADD IF NOT EXISTS PARTITION(
    host = '47.253.49.46',
    dt = '$[yyyy-MM-dd(dd-1)]'
    ) LOCATION '/user/nd_rdg/raw-data/offline/english_moyu/raw_enemy_relationship/47.253.49.46/$[yyyyMMdd(dd-1)]';

ALTER TABLE raw_friend_relationship ADD IF NOT EXISTS PARTITION(
    host = '47.253.49.46',
    dt = '$[yyyy-MM-dd(dd-1)]'
    ) LOCATION '/user/nd_rdg/raw-data/offline/english_moyu/raw_friend_relationship/47.253.49.46/$[yyyyMMdd(dd-1)]';

ALTER TABLE raw_msg_action ADD IF NOT EXISTS PARTITION(
    host = '47.253.49.46',
    dt = '$[yyyy-MM-dd(dd-1)]'
    ) LOCATION '/user/nd_rdg/raw-data/offline/english_moyu/raw_msgaction/47.253.49.46/$[yyyyMMdd(dd-1)]';

ALTER TABLE raw_msg_action2 ADD IF NOT EXISTS PARTITION(
    host = '47.253.49.46',
    dt = '$[yyyy-MM-dd(dd-1)]'
    ) LOCATION '/user/nd_rdg/raw-data/offline/english_moyu/raw_msgaction2/47.253.49.46/$[yyyyMMdd(dd-1)]';

ALTER TABLE raw_msg_type ADD IF NOT EXISTS PARTITION(
    host = '47.253.49.46',
    dt = '$[yyyy-MM-dd(dd-1)]'
    ) LOCATION '/user/nd_rdg/raw-data/offline/english_moyu/raw_msgtype/47.253.49.46/$[yyyyMMdd(dd-1)]';

ALTER TABLE raw_user_equipment ADD IF NOT EXISTS PARTITION(
    host = '47.253.49.46',
    dt = '$[yyyy-MM-dd(dd-1)]'
    ) LOCATION '/user/nd_rdg/raw-data/offline/english_moyu/raw_user_equipment/47.253.49.46/$[yyyyMMdd(dd-1)]';

ALTER TABLE raw_user_eudemons ADD IF NOT EXISTS PARTITION(
    host = '47.253.49.46',
    dt = '$[yyyy-MM-dd(dd-1)]'
    ) LOCATION '/user/nd_rdg/raw-data/offline/english_moyu/raw_user_eudemons/47.253.49.46/$[yyyyMMdd(dd-1)]';

ALTER TABLE raw_user_family ADD IF NOT EXISTS PARTITION(
    host = '47.253.49.46',
    dt = '$[yyyy-MM-dd(dd-1)]'
    ) LOCATION '/user/nd_rdg/raw-data/offline/english_moyu/raw_user_family/47.253.49.46/$[yyyyMMdd(dd-1)]';

ALTER TABLE raw_user_login ADD IF NOT EXISTS PARTITION(
    host = '47.253.49.46',
    dt = '$[yyyy-MM-dd(dd-1)]'
    ) LOCATION '/user/nd_rdg/raw-data/offline/english_moyu/raw_user_login/47.253.49.46/$[yyyyMMdd(dd-1)]';

ALTER TABLE raw_user_extension ADD IF NOT EXISTS PARTITION(
    host = '47.253.49.46',
    dt = '$[yyyy-MM-dd(dd-1)]'
    ) LOCATION '/user/nd_rdg/raw-data/offline/english_moyu/raw_user_extension/47.253.49.46/$[yyyyMMdd(dd-1)]';