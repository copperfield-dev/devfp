import pandas as pd
import hashlib
import os.path

# without action_id
if __name__ == '__main__':
  
  df = pd.read_csv('action_init.csv')
  df.columns = ['cate_id', 'action_id']
  print(df)
  
  map_dict = {}
  
  # build map_id dict
  for index, cate_id in enumerate(df.cate_id.unique(), 1):
    map_dict[str(cate_id)] = index
    
  print(len(map_dict))
  print(map_dict)
  
  mapDf = pd.DataFrame(list(map_dict.items()))
  mapDf.columns = ['cate_id', 'map_id']
  
  mapDf.to_csv('cate_id_map.csv', index=False)
 
  # Generate md5 check file
  file_md5 = None
  if os.path.isfile('cate_id_map.csv'):
    f = open('cate_id_map.csv', 'rb')
    contents=f.read()
    f.close()
    file_md5 = hashlib.md5(contents).hexdigest()
    print(file_md5)

  with open('cate_id_map.md5', 'w') as f:
    f.write(file_md5)
    f.close()