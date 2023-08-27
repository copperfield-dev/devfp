import pandas as pd
import datacompy

if __name__ == '__main__':
    
    # print(len(filter_df.src_uid.unique()))
    # print(len(filter_df.dst_uid.unique()))
    
    
    df_1 = pd.read_csv('data_player_portrait2.csv')
    test1 = df_1[['uid', 'ds']]
    users_1 = df_1.uid.unique()
    print(len(users_1))

    # countDf = test1.groupby('uid').size().reset_index(name='counts')
    # print(countDf[countDf['counts'] > 14])


    df_2 = pd.read_csv('data_behavior_sequence2.csv')
    test2 = df_2[['uid', 'ds']]
    users_2 = df_2.uid.unique()
    print(len(users_2))

    compare = datacompy.Compare(test1, test2, on_index=True)
    print(compare.report())

    df_3 = pd.read_csv('data_label.csv')
    users_3 = df_3.uid.unique()
    print(len(users_3))

    df_4 = pd.read_csv('data_social_network.csv')
    src_df = df_4['src_uid']
    dst_df = df_4['dst_uid']
    users_3 = src_df.append(dst_df).reset_index(name='uid')
    print(len(users_3.uid.unique()))