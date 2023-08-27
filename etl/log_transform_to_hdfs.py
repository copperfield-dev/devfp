import sys
import time
import traceback
import hashlib
import datetime

filename1 = "msgtype "
filename2 = "msgaction "
filename3 = "msgaction2 "
filename4 = "userinfo "
filepath = "/home/wuxin/english-moyu/data"
output_path = "/home/wuxin/moyu_ml/english-moyu/"

ip_to_utc_offset = {
  "74.127.40.35": -6,  # 美国密苏里 圣路易斯, 位于西6区
  "47.253.49.46": -5  # 美国弗吉尼亚, 位于西5区
}


# 用sha256对key来生成唯一性id
def generator_es_id(host, filename, log_date, line_num):
  key = "%s_%s_%s_%s" % (log_date, host, filename, line_num)
  s = hashlib.sha256()
  s.update(key.encode("utf8"))
  return s.hexdigest()


def get_time_from_english_moyu(log_time, host):
  t = time.strptime(log_time, '%H:%M:%S-%y/%m/%d')
  time_zone_offset = ip_to_utc_offset[host]
  return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(time.mktime(t))))


def get_ctz_time_from_english_moyu(log_time, host):
  t = time.strptime(log_time, '%H:%M:%S-%y/%m/%d')
  time_zone_offset = ip_to_utc_offset[host]
  return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(time.mktime(t)) - time_zone_offset * 3600))


def transform_msgaction(filename, date, host):
  try:
    f = open(filename, "r")
    output_filename = "%s%s_%s.%s.log" % (output_path, "msgaction", host, date)
    output_file = open(output_filename, "w")
    index = 0
    for line in f.readlines():
      index = index + 1
      str_list = line.strip().split(" ")
      msgaction_msgtype = "1010"
      action_id = str_list[0]
      user_id = str_list[1]
      msg_time = get_time_from_english_moyu(str_list[3], host)
      msg_time_ctz = get_ctz_time_from_english_moyu(str_list[3], host)
      record = "%s\001%s\001%s\001%s\001%s\001%s" % (
        index, msgaction_msgtype, user_id, action_id, msg_time, msg_time_ctz)
      output_file.write(record)
      output_file.write("\n")
    output_file.close()
    f.close()
  except FileNotFoundError:
    print("%s %s %s file not found" % (filename, date, host))
  except Exception as e:
    print(traceback.format_exc())


def transform_msgaction2(filename, date, host):
  try:
    f = open(filename, "r")
    output_filename = "%s%s_%s.%s.log" % (output_path, "msgaction2", host, date)
    output_file = open(output_filename, "w")
    index = 0
    for line in f.readlines():
      index = index + 1
      str_list = line.strip().split(" ")
      msgaction_msgtype = "1032"
      action_id = str_list[0]
      user_id = str_list[1]
      msg_time = get_time_from_english_moyu(str_list[3], host)
      msg_time_ctz = get_ctz_time_from_english_moyu(str_list[3], host)
      record = "%s\001%s\001%s\001%s\001%s\001%s" % (
        index, msgaction_msgtype, user_id, action_id, msg_time, msg_time_ctz)
      output_file.write(record)
      output_file.write("\n")
    output_file.close()
    f.close()
  except FileNotFoundError:
    print("%s %s %s file not found" % (filename, date, host))
  except Exception as e:
    print(traceback.format_exc())


def transform_msgtype(filename, date, host):
  try:
    f = open(filename, "r")
    output_filename = "%s%s_%s.%s.log" % (output_path, "msgtype", host, date)
    output_file = open(output_filename, "w")
    index = 0
    for line in f.readlines():
      index = index + 1
      str_list = line.strip().split(" ")
      msgaction_msgtype = str_list[0]
      user_id = str_list[1]
      msg_time = get_time_from_english_moyu(str_list[3], host)
      msg_time_ctz = get_ctz_time_from_english_moyu(str_list[3], host)
      record = "%s\001%s\001%s\001%s\001%s" % (index, msgaction_msgtype, user_id, msg_time, msg_time_ctz)
      output_file.write(record)
      output_file.write("\n")
      # 把不满batch_size的剩余的记录都发到es
    
    output_file.close()
    f.close()
  except FileNotFoundError:
    print("%s %s %s file not found" % (filename, date, host))
  except Exception as e:
    print(traceback.format_exc())


def get_friend_relationship_from_line(line, host, index):
  friend_msg = line[len("friend:"):]
  friend_msg_list = friend_msg.split(" ")
  user_id = friend_msg_list[0]
  friend_user_id = friend_msg_list[1]
  msg_time = get_time_from_english_moyu(friend_msg_list[3], host)
  msg_time_ctz = get_ctz_time_from_english_moyu(friend_msg_list[3], host)
  record = "%s\001%s\001%s\001%s\001%s" % (index, user_id, friend_user_id, msg_time, msg_time_ctz)
  return record


def get_enemy_relationship_from_line(line, host, index):
  enemy_msg = line[len("enemy:"):]
  enemy_msg_list = enemy_msg.split(" ")
  user_id = enemy_msg_list[0]
  enemy_user_id = enemy_msg_list[1]
  msg_time = get_time_from_english_moyu(enemy_msg_list[3], host)
  msg_time_ctz = get_ctz_time_from_english_moyu(enemy_msg_list[3], host)
  record = "%s\001%s\001%s\001%s\001%s" % (index, user_id, enemy_user_id, msg_time, msg_time_ctz)
  return record


def get_user_equipment_from_line(line, host, index):
  equipment_msg = line[len("equip:"):]
  equipment_msg_list = equipment_msg.split(" ")
  user_id = equipment_msg_list[0][len("idUser["):-1]
  pos = equipment_msg_list[1][len("pos["):-1]
  item_id = equipment_msg_list[2][len("idItem["):-1]
  level_and_quality_list = equipment_msg_list[3].split(",")
  level = level_and_quality_list[0][len("level["):-1]
  quality = level_and_quality_list[1][len("quality["):-1]
  msg_time = get_time_from_english_moyu(equipment_msg_list[5], host)
  msg_time_ctz = get_ctz_time_from_english_moyu(equipment_msg_list[5], host)
  record = "%s\001%s\001%s\001%s\001%s\001%s\001%s\001%s" % (
    index, user_id, pos, item_id, level, quality, msg_time, msg_time_ctz)
  return record


def get_user_eudemons_from_line(line, host, index):
  eudemons_msg = line[len("eud:"):]
  eudemons_msg_list = eudemons_msg.split(" ")
  user_id = eudemons_msg_list[0][len("idUser["):-1]
  eudemons_id = eudemons_msg_list[1][len("idEud["):-1]
  level = eudemons_msg_list[2][len("level["):-1]
  life = eudemons_msg_list[3][len("life["):-1]
  def_msg = eudemons_msg_list[4][len("def["):-1]
  mdef = eudemons_msg_list[5][len("mdef["):-1]
  msg_time = get_time_from_english_moyu(eudemons_msg_list[7], host)
  msg_time_ctz = get_ctz_time_from_english_moyu(eudemons_msg_list[7], host)
  record = "%s\001%s\001%s\001%s\001%s\001%s\001%s\001%s\001%s" % (
    index, user_id, eudemons_id, level, life, def_msg, mdef, msg_time, msg_time_ctz)
  return record


def get_user_family_from_line(line, host, index):
  user_family_list = line.split(" ")
  user_id = user_family_list[0][len("idUser["):-1]
  syn_id = user_family_list[1][len("idSyn["):-1]
  mate_id = user_family_list[2][len("idMate["):-1]
  family_id = user_family_list[3][len("idFamily["):-1]
  msg_time = get_time_from_english_moyu(user_family_list[5], host)
  msg_time_ctz = get_ctz_time_from_english_moyu(user_family_list[5], host)
  record = "%s\001%s\001%s\001%s\001%s\001%s\001%s" % (
    index, user_id, syn_id, mate_id, family_id, msg_time, msg_time_ctz)
  return record


def get_user_login_from_line(line, host, index):
  user_login_list = line.split(" ")
  user_id = user_login_list[0][len("idUser["):-1]
  account_id = user_login_list[1][len("idAccount["):-1]
  prof = user_login_list[2][len("prof["):-1]
  level = user_login_list[3][len("level["):-1]
  vip = user_login_list[4][len("vip["):-1]
  battle = user_login_list[5][len("battle["):-1]
  lastlogin = user_login_list[6][len("lastlogin["):-1]
  msg_time = get_time_from_english_moyu(user_login_list[8], host)
  msg_time_ctz = get_ctz_time_from_english_moyu(user_login_list[8], host)
  record = "%s\001%s\001%s\001%s\001%s\001%s\001%s\001%s\001%s\001%s" % (
    index, user_id, account_id, prof, level, vip, battle, lastlogin, msg_time, msg_time_ctz)
  return record


def get_user_extension_from_line(line, host, index):
  user_extension_list = line.split(" ")
  user_id = user_extension_list[0][len("idUser["):-1]
  face_id = user_extension_list[1][len("face["):-1]
  hair_id = user_extension_list[2][len("hair["):-1]
  magic_stone_num = user_extension_list[3][len("em["):-1]
  gold_num = user_extension_list[4][len("m["):-1]
  msg_time = get_time_from_english_moyu(user_extension_list[6], host)
  msg_time_ctz = get_ctz_time_from_english_moyu(user_extension_list[6], host)
  record = "%s\001%s\001%s\001%s\001%s\001%s\001%s\001%s" % (
    index, user_id, face_id, hair_id, magic_stone_num, gold_num, msg_time, msg_time_ctz
  )
  return record


def get_record_from_userinfo_line(line, host, index):
  # 属于好友关系记录
  record = None
  msg_type = None
  if line.startswith("friend:"):
    msg_type = "friend_relationship"
    record = get_friend_relationship_from_line(line, host, index)
  elif line.startswith("enemy:"):
    msg_type = "enemy_relationship"
    record = get_enemy_relationship_from_line(line, host, index)
  elif line.startswith("equip:"):
    msg_type = "user_equipment"
    record = get_user_equipment_from_line(line, host, index)
  elif line.startswith("eud:"):
    msg_type = "user_eudemons"
    record = get_user_eudemons_from_line(line, host, index)
  elif line.startswith("idUser["):
    str_list = line.split(" ")
    if str_list[1].startswith("idAccount["):
      msg_type = "user_login"
      record = get_user_login_from_line(line, host, index)
    elif str_list[1].startswith("idSyn["):
      msg_type = "user_family"
      record = get_user_family_from_line(line, host, index)
    elif str_list[1].startswith("face["):
      msg_type = "user_extension"
      record = get_user_extension_from_line(line, host, index)
  else:
    msg_type = ""
    record = None
  
  return msg_type, record


def transform_userinfo(filename, date, host):
  try:
    f = open(filename, "r")
    output_friend_relationship_filename = "%s%s_%s.%s.log" % (output_path, "friend_relationship", host, date)
    output_friend_relationship_file = open(output_friend_relationship_filename, "w")
    output_enemy_relationship_filename = "%s%s_%s.%s.log" % (output_path, "enemy_relationship", host, date)
    output_enemy_relationship_file = open(output_enemy_relationship_filename, "w")
    output_user_equipment_filename = "%s%s_%s.%s.log" % (output_path, "user_equipment", host, date)
    output_user_equipment_file = open(output_user_equipment_filename, "w")
    output_user_eudemons_filename = "%s%s_%s.%s.log" % (output_path, "user_eudemons", host, date)
    output_user_eudemons_file = open(output_user_eudemons_filename, "w")
    output_user_family_filename = "%s%s_%s.%s.log" % (output_path, "user_family", host, date)
    output_user_family_file = open(output_user_family_filename, "w")
    output_user_login_filename = "%s%s_%s.%s.log" % (output_path, "user_login", host, date)
    output_user_login_file = open(output_user_login_filename, "w")
    output_user_extension_filename = "%s%s_%s.%s.log" % (output_path, "user_extension", host, date)
    output_user_extension_file = open(output_user_extension_filename, "w")
    record_type_to_file = {
      "friend_relationship": output_friend_relationship_file,
      "enemy_relationship": output_enemy_relationship_file,
      "user_equipment": output_user_equipment_file,
      "user_eudemons": output_user_eudemons_file,
      "user_family": output_user_family_file,
      "user_login": output_user_login_file,
      "user_extension": output_user_extension_file
    }
    
    content = f.readlines()
    for index, line in enumerate(content):
      msg_type, record = get_record_from_userinfo_line(line.strip(), host, index)
      file = record_type_to_file.get(msg_type, None)
      if file is not None:
        file.write(record)
        file.write("\n")
      else:
        print("line not match any message format : %s" % (line.strip()))
    output_friend_relationship_file.close()
    output_user_equipment_file.close()
    output_user_eudemons_file.close()
    output_user_family_file.close()
    output_user_login_file.close()
    output_enemy_relationship_file.close()
    f.close()
  except FileNotFoundError as e:
    print(traceback.format_exc())
  except Exception as e:
    print(traceback.format_exc())


if __name__ == "__main__":
  
  if len(sys.argv) < 3:
    print("usage %s date host" % (sys.argv[0]))
    sys.exit(-1)
  log_date = sys.argv[1]
  host = sys.argv[2]
  if ip_to_utc_offset[host] is None:
    print("ip not in the map, could not transform time by the timezone")
    sys.exit(-1)
 

  moyu_date = datetime.datetime.strptime(log_date, '%Y-%m-%d').strftime('%Y%m%d')
  
  msgaction_filename = "%s/%s/da/%s/%s%s.log" % (filepath, host, moyu_date, filename2, log_date)
  print("start transform_msgaction %s" % (msgaction_filename))
  transform_msgaction(msgaction_filename, log_date, host)
  msgaction2_filename = "%s/%s/da/%s/%s%s.log" % (filepath, host, moyu_date, filename3, log_date)
  print("start transform_msgaction2 %s" % (msgaction2_filename))
  transform_msgaction2(msgaction2_filename, log_date, host)
  msgtype_filename = "%s/%s/da/%s/%s%s.log" % (filepath, host, moyu_date, filename1, log_date)
  print("start transform_msgtype %s" % (msgtype_filename))
  transform_msgtype(msgtype_filename, log_date, host)
  userinfo_filename = "%s/%s/da/%s/%s%s.log" % (filepath, host, moyu_date, filename4, log_date)
  print("start transform_userinfo %s" % (userinfo_filename))
  transform_userinfo(userinfo_filename, log_date, host) 
