#!/bin/bash
hive -e 'select * from ywmy.data_player_portrait_7_to_7' | sed 's/[\t]/,/g' > data_player_portrait_7_to_7.csv
hive -e 'select * from ywmy.data_player_portrait_14_to_7' | sed 's/[\t]/,/g' > data_player_portrait_14_to_7.csv
hive -e 'select * from ywmy.data_player_portrait_14_to_14' | sed 's/[\t]/,/g' > data_player_portrait_14_to_14.csv

hive -e 'select * from ywmy.data_behavior_sequence_7_to_7' > data_behavior_sequence_7_to_7.csv
hive -e 'select * from ywmy.data_behavior_sequence_14_to_7' | sed 's/[\t]/,/g' > data_behavior_sequence_14_to_7.csv
hive -e 'select * from ywmy.data_behavior_sequence_14_to_14' | sed 's/[\t]/,/g' > data_behavior_sequence_14_to_14.csv

hive -e 'select * from ywmy.tmp_social_player_7_to_7' | sed 's/[\t]/,/g' > tmp_social_player_7_to_7.csv
hive -e 'select * from ywmy.tmp_social_player_14_to_7' | sed 's/[\t]/,/g' > tmp_social_player_14_to_7.csv
hive -e 'select * from ywmy.tmp_social_player_14_to_14' | sed 's/[\t]/,/g' > tmp_social_player_14_to_14.csv

hive -e 'select * from ywmy.data_social_network_7days_before' | sed 's/[\t]/,/g' > data_social_network_7days_before.csv
hive -e 'select * from ywmy.data_social_network_14days_before' | sed 's/[\t]/,/g' > data_social_network_14days_before.csv

hive -e 'select * from ywmy.data_label_7_to_7' | sed 's/[\t]/,/g' > data_label_7_to_7.csv
hive -e 'select * from ywmy.data_label_14_to_7' | sed 's/[\t]/,/g' > data_label_14_to_7.csv
hive -e 'select * from ywmy.data_label_14_to_14' | sed 's/[\t]/,/g' > data_label_14_to_14.csv

hive -e 'select * from ywmy.tmp_player_7_to_7' | sed 's/[\t]/,/g' > tmp_player_7_to_7.csv
hive -e 'select * from ywmy.tmp_player_14_to_7' | sed 's/[\t]/,/g' > tmp_player_14_to_7.csv
hive -e 'select * from ywmy.tmp_player_14_to_14' | sed 's/[\t]/,/g' > tmp_player_14_to_14.csv