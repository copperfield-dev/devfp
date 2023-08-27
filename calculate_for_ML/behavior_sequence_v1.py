import pandas as pd
import sys
import datetime
from itertools import product

# 计算7天预测7天的行为数据
def calculate_7_to_7(dt):
  df_7_to_7 = pd.read_csv('../data/input/data_behavior_sequence_7_to_7.csv', sep='\t', header=None)
  df_7_to_7.columns = ['uid', 'ds', 'seq']
  df_7_to_7['ds'] = pd.to_datetime(df_7_to_7['ds'])
  
  tmp_user_df_7_to_7 = pd.read_csv('../data/input/tmp_player_7_to_7.csv', header=None)
  tmp_user_df_7_to_7.columns = ['uid']
  
  join_df_7_to_7 = df_7_to_7.merge(tmp_user_df_7_to_7, on=['uid'], how='right')
  
  start_date_7_to_7 = dt + datetime.timedelta(days=-14)
  date_list_7_to_7 = pd.date_range(start_date_7_to_7, periods=7)
  print(date_list_7_to_7)
  
  join_df_7_to_7.sort_values("uid", inplace=True)
  
  users_7_to_7 = join_df_7_to_7.uid.unique()
  df1 = pd.DataFrame(list(product(users_7_to_7, date_list_7_to_7)), columns=['uid', 'ds'])
  
  result_df_7_to_7 = df1.merge(join_df_7_to_7, how='left').fillna(value="")
  print(result_df_7_to_7)
  result_df_7_to_7.to_csv('../data/output/data_behavior_sequence_7_to_7.csv', index=False)


# 计算14天预测7天的行为数据
def calculate_14_to_7(dt):
  df_14_to_7 = pd.read_csv('data_behavior_sequence_14_to_7.csv', header=None)
  df_14_to_7.columns = ['uid', 'ds', 'seq']
  df_14_to_7['ds'] = pd.to_datetime(df_14_to_7['ds'])
  
  tmp_user_df_14_to_7 = pd.read_csv('tmp_player_14_to_7.csv', header=None)
  tmp_user_df_14_to_7.columns = ['uid', 'ds']
  tmp_user_df_14_to_7['ds'] = pd.to_datetime(tmp_user_df_14_to_7['ds'])
  
  join_df_14_to_7 = df_14_to_7.merge(tmp_user_df_14_to_7, on=['uid', 'ds'], how='right')
  
  start_date_14_to_7 = dt + datetime.timedelta(days=-21)
  date_list_14_to_7 = pd.date_range(start_date_14_to_7, periods=14)
  print(date_list_14_to_7)
  
  join_df_14_to_7.sort_values("uid", inplace=True)
  
  users_14_to_7 = join_df_14_to_7.uid.unique()
  df1 = pd.DataFrame(list(product(users_14_to_7, date_list_14_to_7)), columns=['uid', 'ds'])
  result_df_14_to_7 = df1.merge(join_df_14_to_7, how='left').fillna(value="")
  print(result_df_14_to_7)
  result_df_14_to_7.to_csv('data/data_behavior_sequence_14_to_7.csv', index=False)


# 计算14天预测14天的行为数据
def calculate_14_to_14(dt):
  df_14_to_14 = pd.read_csv('data_behavior_sequence_14_to_14.csv', header=None)
  df_14_to_14.columns = ['uid', 'ds', 'seq']
  df_14_to_14['ds'] = pd.to_datetime(df_14_to_14['ds'])
  
  tmp_user_df_14_to_14 = pd.read_csv('tmp_player_14_to_14.csv', header=None)
  tmp_user_df_14_to_14.columns = ['uid', 'ds']
  tmp_user_df_14_to_14['ds'] = pd.to_datetime(tmp_user_df_14_to_14['ds'])
  
  join_df_14_to_14 = df_14_to_14.merge(tmp_user_df_14_to_14, on=['uid', 'ds'], how='right')
  
  start_date_14_to_14 = dt + datetime.timedelta(days=-28)
  date_list_14_to_14 = pd.date_range(start_date_14_to_14, periods=14)
  print(date_list_14_to_14)
  
  join_df_14_to_14.sort_values("uid", inplace=True)
  
  users_14_to_14 = join_df_14_to_14.uid.unique()
  df1 = pd.DataFrame(list(product(users_14_to_14, date_list_14_to_14)), columns=['uid', 'ds'])
  result_df_14_to_14 = df1.merge(join_df_14_to_14, how='left').fillna(value="")
  print(result_df_14_to_14)
  result_df_14_to_14.to_csv('data/data_behavior_sequence_14_to_14.csv', index=False)


if __name__ == '__main__':
  if len(sys.argv) < 3:
    print("usage %s date host" % (sys.argv[0]))
    sys.exit(-1)
  
  date_str = sys.argv[1]
  host = sys.argv[2]
  dt = datetime.datetime.strptime(date_str, '%Y-%m-%d')
  
  calculate_7_to_7(dt)
  # calculate_14_to_7()
  # calculate_14_to_14()