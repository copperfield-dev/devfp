import pandas as pd
import os.path
import hashlib

# without action_id
if __name__ == '__main__':
  
  df = pd.read_csv('data/june/data_behavior_sequence_0620.csv', sep='\t')
  df.columns = ['user_id', 'dt', 'cate_id', 'action_id', 'host']
  
  # map dict md5 check
  f = open('cate_id_map.md5')
  map_md5 = f.read()
  f.close()
  
  file_md5 = None
  if os.path.isfile('cate_id_map.csv'):
    f = open('cate_id_map.csv', 'rb')
    contents = f.read()
    f.close()
    file_md5 = hashlib.md5(contents).hexdigest()
  
  if file_md5 != map_md5:
    print("Error: File is changed! ")
    exit(0)
  
  id_map_df = pd.read_csv('cate_id_map.csv', header=0)
  max = id_map_df["map_id"].max()
  
  id_map_dict = dict(zip(id_map_df['cate_id'], id_map_df['map_id']))
  map_dict_new = {}
  
  
  def map_func(cate_id):
    if cate_id in id_map_dict:
      return str(id_map_dict[cate_id])
    else:
      global max
      map_id = max + 1
      max = map_id
      print("change max:" + str(max))
      id_map_dict[cate_id] = map_id
      
      map_dict_new[cate_id] = map_id
      return str(map_id)
  
  
  df["map_id"] = df["cate_id"].apply(map_func)
  
  count_df = df.groupby(["user_id", "dt", "map_id"])["action_id"].count().reset_index(name="nums")
  result_df = count_df.sort_values("nums", ascending=False).groupby(["user_id", "dt"])["map_id"].apply(list).reset_index(
    name="behavior_sequence")
 
  result_df["behavior_sequence"] = result_df["behavior_sequence"].apply(', '.join)
  print(result_df)
  # result_df.to_csv('data_behavior_sequence2_7d_without_actionid.csv', mode='a', index=False)
  result_df.to_csv('data_behavior_sequence2_14d_without_actionid.csv', mode='a', index=False, header=False)
  
  # 写入新出现的cate_id&action_id到字典
  if len(map_dict_new) != 0:
    map_new_df = pd.DataFrame(list(map_dict_new.items()))
    map_new_df.columns = ['cate_id', 'map_id']
    id_map_df = id_map_df.append(map_new_df, ignore_index=True)
    
    os.rename("cate_id_map.csv", "cate_id_map.csv.backup")
    os.rename("cate_id_map.md5", "cate_id_map.md5.backup")
    id_map_df.to_csv('cate_id_map.csv', index=False)
    
    # 生成新的MD5校验文件
    f = open('cate_id_map.csv', 'rb')
    contents = f.read()
    f.close()
    file_md5 = hashlib.md5(contents).hexdigest()
    print(file_md5)
    
    with open('cate_id_map.md5', 'w') as f:
      f.write(file_md5)
      f.close()
    
    os.remove("cate_id_map.csv.backup")
    os.remove("cate_id_map.md5.backup")
