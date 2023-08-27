import pandas as pd
import hashlib
import os.path

if __name__ == '__main__':
  
  df = pd.read_csv('action_init.csv')
  df.columns = ['cate_id', 'action_id']
  
  df_1032 = df[df['cate_id'] == 1032].sort_values('action_id').action_id.unique()
  df_1010 = df[df['cate_id'] == 1010].sort_values('action_id').action_id.unique()
  df_other = df[(df['cate_id'] != 1032) & (df['cate_id'] != 1010)].sort_values('action_id').cate_id.unique()

  map_dict = {}
  
  # build map_id dict
  
  # process cate_id is 1032
  for index, action_id in enumerate(df_1032, 1):
    map_dict['1032-' + str(action_id)] = index

  # process cate_id is 1010
  for index, action_id in enumerate(df_1010, 1):
    map_dict['1010-' + str(action_id)] = index + 500
    
  # process other cate_id
  for index, cate_id in enumerate(df_other, 1):
    map_dict[str(cate_id)] = index + 800
    
  print(len(map_dict))
  # print(map_dict)
  
  mapDf = pd.DataFrame(list(map_dict.items()))
  mapDf.columns = ['join_id', 'map_id']
  
  mapDf.to_csv('cate_action_id_map.csv', index=False)
 
  # Generate md5 check file
  file_md5 = None
  if os.path.isfile('cate_action_id_map.csv'):
    f = open('cate_action_id_map.csv', 'rb')
    contents=f.read()
    f.close()
    file_md5 = hashlib.md5(contents).hexdigest()
    print(file_md5)
    
  with open('cate_action_id_map.md5', 'w') as f:
    f.write(file_md5)
    f.close()