import pandas as pd
import os.path
import hashlib

if __name__ == '__main__':
  
  df = pd.read_csv('data/june/data_behavior_sequence_0613.csv', sep='\t')
  df.columns = ['user_id', 'dt', 'cate_id', 'action_id', 'host']
  
  # map dict md5 check
  f = open('cate_action_id_map.md5')
  map_md5 = f.read()
  f.close()
  
  file_md5 = None
  if os.path.isfile('cate_action_id_map.csv'):
    f = open('cate_action_id_map.csv', 'rb')
    contents = f.read()
    f.close()
    file_md5 = hashlib.md5(contents).hexdigest()
  
  if file_md5 != map_md5:
    print("Error: File is changed! ")
    exit(0)
  
  id_map_df = pd.read_csv('cate_action_id_map.csv', header=0)
  max_1032 = id_map_df[id_map_df.join_id.str.startswith('1032-')]["map_id"].max()
  max_1010 = id_map_df[id_map_df.join_id.str.startswith('1010-')]["map_id"].max()
  max_other = id_map_df[~id_map_df.join_id.str.startswith('1032-') & ~id_map_df.join_id.str.startswith('1010-')][
    "map_id"].max()
  
  id_map_dict = dict(zip(id_map_df['join_id'], id_map_df['map_id']))
  map_dict_new = {}
  
  
  def map_func(cate_id, action_id):
    if cate_id == 1032 or cate_id == 1010:
      join_id = str(cate_id) + "-" + str(action_id)
    else:
      join_id = str(cate_id)
    
    if join_id in id_map_dict:
      return str(id_map_dict[join_id])
    else:
      if cate_id == 1032:
        global max_1032
        map_id = max_1032 + 1
        max_1032 = map_id
        print("change 1032:" + str(max_1032))
        id_map_dict[join_id] = map_id
      elif cate_id == 1010:
        global max_1010
        map_id = max_1010 + 1
        max_1010 = map_id
        print("change 1010:" + str(max_1010))
        id_map_dict[join_id] = map_id
      else:
        global max_other
        map_id = max_other + 1
        max_other = map_id
        print("change other:" + str(max_other))
        id_map_dict[join_id] = map_id
      
      map_dict_new[join_id] = map_id
      return str(map_id)
  
  
  df["map_id"] = df[["cate_id", "action_id"]].apply(lambda row: map_func(row['cate_id'], row['action_id']), axis=1)
  
  count_df = df.groupby(["user_id", "dt", "map_id"])["action_id"].count().reset_index(name="nums")
  
  result_df = count_df.sort_values("nums", ascending=False).groupby(["user_id", "dt"])["map_id"].apply(list).reset_index(
    name="behavior_sequence")
  result_df["behavior_sequence"] = result_df["behavior_sequence"].apply(lambda x: x[:64]).apply(', '.join)
  print(result_df)
  
  # result_df.to_csv('data_behavior_sequence2_7d.csv', mode='a', index=False)
  result_df.to_csv('data_behavior_sequence2_7d.csv', mode='a', index=False, header=False)
  
  # 写入新出现的cate_id&action_id到字典
  if len(map_dict_new) != 0:
    map_new_df = pd.DataFrame(list(map_dict_new.items()))
    map_new_df.columns = ['join_id', 'map_id']
    id_map_df = id_map_df.append(map_new_df, ignore_index=True)
    
    os.rename("cate_action_id_map.csv", "cate_action_id_map.csv.backup")
    os.rename("cate_action_id_map.md5", "cate_action_id_map.md5.backup")
    id_map_df.to_csv('cate_action_id_map.csv', index=False)
    
    # 生成新的MD5校验文件
    f = open('cate_action_id_map.csv', 'rb')
    contents = f.read()
    f.close()
    file_md5 = hashlib.md5(contents).hexdigest()
    print(file_md5)
    
    with open('cate_action_id_map.md5', 'w') as f:
      f.write(file_md5)
      f.close()
    
    os.remove("cate_action_id_map.csv.backup")
    os.remove("cate_action_id_map.md5.backup")
