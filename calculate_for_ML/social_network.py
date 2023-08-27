import pandas as pd

# 计算7天预测7天的好友关系数据
def calculate_7_to_7():
    df_7_to_7 = pd.read_csv('../data/input/tmp_social_player_7_to_7.csv', header=None)
    df_7_to_7.columns = ['uid']
    users_7_to_7 = df_7_to_7.uid.unique()
    print(len(users_7_to_7))
    social_df_7_to_7 = pd.read_csv('../data/input/data_social_network_7days_before.csv', header=None)
    social_df_7_to_7.columns = ['user_id', 'friend_user_id', 'weight', 'host']
    filter_df = social_df_7_to_7[['user_id', 'friend_user_id', 'weight']][
        (social_df_7_to_7['user_id'].isin(users_7_to_7) & social_df_7_to_7['friend_user_id'].isin(users_7_to_7))]
    filter_df.columns = ['src_uid', 'dst_uid', 'weight']
    tmp_user_df_7_to_7 = pd.read_csv('../data/input/tmp_player_7_to_7.csv', header=None)
    tmp_user_df_7_to_7.columns = ['uid']
    # 列出不在social network中的uid，把这部分数据补上
    other = list(set(tmp_user_df_7_to_7.uid.unique()).difference(set(filter_df.src_uid.unique())))
    print(other)
    index = len(filter_df)
    for gamer in other:
        filter_df.loc[index] = [gamer, default_user_id, '0']
        index += 1
    filter_df.to_csv('../data/output/data_social_network_7_to_7.csv', index=False)


# * 计算14天预测7天的好友关系数据
def calculate_14_to_7():
    global df_14_to_7
    df_14_to_7 = pd.read_csv('tmp_social_player_14_to_7.csv', header=None)
    df_14_to_7.columns = ['uid']
    users_14_to_7 = df_14_to_7.uid.unique()
    print(len(users_14_to_7))
    social_df_14_to_7 = pd.read_csv('data_social_network_7days_before.csv', header=None)
    social_df_14_to_7.columns = ['user_id', 'friend_user_id', 'weight', 'host']
    filter_df = social_df_14_to_7[['user_id', 'friend_user_id', 'weight']][
        (social_df_14_to_7['user_id'].isin(users_14_to_7) & social_df_14_to_7['friend_user_id'].isin(users_14_to_7))]
    filter_df.columns = ['src_uid', 'dst_uid', 'weight']
    tmp_user_df_14_to_7 = pd.read_csv('tmp_player_14_to_7.csv', header=None)
    tmp_user_df_14_to_7.columns = ['uid', 'ds']
    tmp_user_df_14_to_7['ds'] = pd.to_datetime(tmp_user_df_14_to_7['ds'])
    # 列出不在social network中的uid，把这部分数据补上
    other = list(set(tmp_user_df_14_to_7.uid.unique()).difference(set(filter_df.src_uid.unique())))
    print(other)
    index = len(filter_df)
    for gamer in other:
        filter_df.loc[index] = [gamer, default_user_id, '0']
        index += 1
    filter_df.to_csv('data/data_social_network_14_to_7.csv', index=False)


# * 计算14天预测14天的好友关系数据
def calculate_14_to_14():
    global df_14_to_14
    df_14_to_14 = pd.read_csv('tmp_social_player_14_to_14.csv', header=None)
    df_14_to_14.columns = ['uid']
    users_14_to_14 = df_14_to_14.uid.unique()
    print(len(users_14_to_14))
    social_df_14_to_14 = pd.read_csv('data_social_network_14days_before.csv', header=None)
    social_df_14_to_14.columns = ['user_id', 'friend_user_id', 'weight', 'host']
    filter_df = social_df_14_to_14[['user_id', 'friend_user_id', 'weight']][
        (social_df_14_to_14['user_id'].isin(users_14_to_14) & social_df_14_to_14['friend_user_id'].isin(
            users_14_to_14))]
    filter_df.columns = ['src_uid', 'dst_uid', 'weight']
    tmp_user_df_14_to_14 = pd.read_csv('tmp_player_14_to_14.csv', header=None)
    tmp_user_df_14_to_14.columns = ['uid', 'ds']
    tmp_user_df_14_to_14['ds'] = pd.to_datetime(tmp_user_df_14_to_14['ds'])
    # 列出不在social network中的uid，把这部分数据补上
    other = list(set(tmp_user_df_14_to_14.uid.unique()).difference(set(filter_df.src_uid.unique())))
    print(other)
    index = len(filter_df)
    for gamer in other:
        filter_df.loc[index] = [gamer, default_user_id, '0']
        index += 1
    filter_df.to_csv('data/data_social_network_14_to_14.csv', index=False)


if __name__ == '__main__':
    default_user_id = '1649411'
    
    calculate_7_to_7()
    # calculate_14_to_7()
    # calculate_14_to_14()