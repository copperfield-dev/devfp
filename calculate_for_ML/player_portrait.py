import pandas as pd
import sys
import datetime
from itertools import product

# 计算7天预测7天的用户属性数据
def calculate_7_to_7(dt):
    df_7_to_7 = pd.read_csv('../data/input/data_player_portrait_7_to_7.csv', header=None)
    df_7_to_7.columns = ['uid', 'ds', 'prof', 'level', 'vip', 'battle', 'face_id', 'hair_id', 'magic_stone_num',
                         'gold_num', 'host']
    df_7_to_7 = df_7_to_7.drop(columns=['host'])
    # df_7_to_7.drop_duplicates(subset=['uid', 'ds'], inplace=True)
    df_7_to_7['ds'] = pd.to_datetime(df_7_to_7['ds'])
    start_date_7_to_7 = dt + datetime.timedelta(days=-14)
    date_list_7_to_7 = pd.date_range(start_date_7_to_7, periods=7)
    users_7_to_7 = df_7_to_7.uid.unique()
    print(date_list_7_to_7)
    df1 = pd.DataFrame(list(product(users_7_to_7, date_list_7_to_7)), columns=['uid', 'ds'])
    print(df1)

    df_7_to_7 = df1.merge(df_7_to_7, how='left').groupby('uid').apply(lambda x: x.ffill().bfill())
    print(df_7_to_7[0:14])
    # df_test = df_7_to_7.groupby(['uid', 'ds']).size()
    # df_test.to_csv('../data/output/test.csv')
    # print(df_test)
    df_7_to_7.to_csv('../data/output/data_player_portrait_7_to_7.csv', index=False)


def calculate_14_to_7(dt):
    df_14_to_7 = pd.read_csv('data_player_portrait_14_to_7.csv', header=None)
    df_14_to_7.columns = ['uid', 'ds', 'prof', 'level', 'vip', 'battle', 'face_id', 'hair_id', 'magic_stone_num',
                          'gold_num']
    df_14_to_7['ds'] = pd.to_datetime(df_14_to_7['ds'])
    start_date_14_to_7 = dt + datetime.timedelta(days=-21)
    end_date_14_to_7 = dt + datetime.timedelta(days=-7)
    date_list_14_to_7 = pd.date_range(start_date_14_to_7, periods=7)
    users_14_to_7 = df_14_to_7.uid.unique()
    print(date_list_14_to_7)
    df2 = pd.DataFrame(list(product(users_14_to_7, date_list_14_to_7)), columns=['uid', 'ds'])
    print(df2)
    df_14_to_7 = df2.merge(df_14_to_7, how='left').bfill()
    df_14_to_7.to_csv('data/data_player_portrait_14_to_7.csv', index=False)


def calculate_14_to_14(dt):
    df_14_to_14 = pd.read_csv('data_player_portrait_14_to_14.csv', header=None)
    df_14_to_14.columns = ['uid', 'ds', 'prof', 'level', 'vip', 'battle', 'face_id', 'hair_id', 'magic_stone_num',
                           'gold_num']
    df_14_to_14['ds'] = pd.to_datetime(df_14_to_14['ds'])
    start_date_14_to_14 = dt + datetime.timedelta(days=-28)
    end_date_14_to_14 = dt + datetime.timedelta(days=-28)
    date_list_14_to_14 = pd.date_range(start_date_14_to_14, periods=7)
    users_14_to_14 = df_14_to_14.uid.unique()
    print(date_list_14_to_14)
    df3 = pd.DataFrame(list(product(users_14_to_14, date_list_14_to_14)), columns=['uid', 'ds'])
    print(df3)
    df_14_to_14 = df3.merge(df_14_to_14, how='left').bfill()
    df_14_to_14.to_csv('data/data_player_portrait_14_to_14.csv', index=False)


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("usage %s date host" % (sys.argv[0]))
        sys.exit(-1)
      
    date_str = sys.argv[1]
    host = sys.argv[2]
    dt = datetime.datetime.strptime(date_str, '%Y-%m-%d')

    calculate_7_to_7(dt)
    # calculate_14_to_7(dt)
    # calculate_14_to_14(dt)
