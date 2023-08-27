class FtpClient:
  
  def __init__(self, host, port, username, password):
    self.host = host
    self.port = port
    self.username = username
    self.password = password

if __name__ == '__main__':
  my_ftp = FtpClient('192.168.243.145', 21, 'test', 'test')
  print(my_ftp)