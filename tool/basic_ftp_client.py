from ftplib import FTP
import os
import sys
import time
import re

from ftp_client import FtpClient

class BasicFtpClient(FtpClient):
  
  def __init__(self, host, port, username, password):
    super().__init__(host, port, username, password)
    self.log_file = open("log.txt", "a")
    self.client = self.__login()
  
  def __login(self):
    ftp = FTP()
    try:
      # socket.setdefaulttimeout(self.timeout)
      
      # 打开调试级别2，显示详细信息
      ftp.set_debuglevel(2)
      
      self.debug_print('开始尝试连接到 %s' % self.host)
      ftp.connect(host=self.host, port=self.port)
      self.debug_print('成功连接到 %s' % self.host)
      
      self.debug_print('开始尝试登录到 %s' % self.host)
      ftp.login(self.username, self.password)
      self.debug_print('成功登录到 %s' % self.host)
      
      self.debug_print(ftp.getwelcome())
    except Exception as err:
      self.deal_error("FTP 连接或登录失败 ，错误描述为：%s" % err)
      pass
    
    return ftp
  
  def list_dir(self, path='.'):
    self.client.dir(path)
  
  def download_file(self, local_file, remote_file, buffer_size=1024):
    # 本地是否有此文件，来确认是否启用断点续传
    if not os.path.exists(local_file):
      with open(local_file, 'wb') as f:
        self.client.retrbinary('RETR %s' % remote_file, f.write, buffer_size)
        f.close()
        # client.set_debuglevel(0)             #关闭调试模式
        return True
    else:
      p = re.compile(r'\\', re.S)
      local_file = p.sub('/', local_file)
      local_size = os.path.getsize(local_file)
      with open(local_file, 'ab+') as f:
        self.client.retrbinary('RETR %s' % remote_file, f.write, buffer_size, local_size)
        f.close()
        # ftp.set_debuglevel(0)             #关闭调试模式
        return True
  
  def download_dir(self, local_path, remote_path):
    if not os.path.exists(local_path):
      os.makedirs(local_path)
    
    self.client.cwd(remote_path)
    
    # 获取该目录下所有文件名，列表形式
    for file in self.client.nlst():
      print(file)
      local_file = os.path.join(local_path, file)  # 下载到当地的全路径
      if file.find('.') == -1:  # 判断是否为子目录
        if not os.path.exists(local_file):
          os.makedirs(local_file)
          self.download_dir(local_file, file)  # 下载子目录
      else:
        # 将传输模式改为二进制模式 ,避免提示 ftplib.error_perm: 550 SIZE not allowed in ASCII
        self.client.voidcmd('TYPE I')
        buffer_size = self.client.size(file)  # 服务器里的文件总大小
        print(buffer_size)
        self.download_file(local_file, file, buffer_size)
    self.client.cwd("..")  # 返回路径最外侧
    return
  
  def debug_print(self, s):
    self.write_log(s)
  
  def deal_error(self, e):
    log_str = '发生错误: %s' % e
    self.write_log(log_str)
    sys.exit()
  
  def write_log(self, log_str):
    time_now = time.localtime()
    date_now = time.strftime('%Y-%m-%d', time_now)
    format_log_str = "%s ---> %s \n " % (date_now, log_str)
    print(format_log_str)
    self.log_file.write(format_log_str)
  
  def close(self):
    self.debug_print("close()---> FTP退出")
    self.client.quit()
    self.log_file.close()


if __name__ == "__main__":
  my_ftp = BasicFtpClient('192.168.243.145', 21, 'test', 'test')
  my_ftp.list_dir('hello')
  # my_ftp.download_file('/Users/copperfield/DATA MINING.pdf', 'hello/DATA MINING.pdf')
  my_ftp.download_dir('/Users/copperfield/hello', 'hello')