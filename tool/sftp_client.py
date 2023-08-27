import os.path
import sys

#sys.path.append("../.venv/lib/python3.10/site-packages")
import paramiko
from ftp_client import FtpClient

class SftpClient(FtpClient):
    def __init__(self, host, port, username, password):
        super().__init__(host, port, username, password)
        self.client = self.__login()

    def __login(self):
        # 建立连接
        transport = paramiko.Transport(sock=(self.host, self.port))
        # 登录
        transport.connect(username=self.username, password=self.password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        return sftp

    def __copy_to_hdfs(args):
        pass

    def list_dir(self, path='.'):
        print(self.client.listdir(path))

    def download_file(self, local_file, remote_file):
        # 判断本地是否存在文件
        assert not os.path.exists(local_file)
        with open(local_file, 'wb') as f:
            self.client.get(remote_file, local_file)
            f.close()

    def download_dir(self, local_path, remote_path):
        if not os.path.exists(local_path):
            os.makedirs(local_path)

        self.client.chdir(remote_path)

        # 获取该目录下所有文件名，列表形式
        for file in self.client.listdir():
            print(file)
            local_file = os.path.join(local_path, file)  # 下载到当地的全路径
            if file.find('.') == -1:  # 判断是否为子目录
                if not os.path.exists(local_file):
                    os.makedirs(local_file)
                    self.download_dir(local_file, file)  # 下载子目录
            else:
                self.download_file(local_file, file)
        self.client.chdir("..")  # 返回路径最外侧
        return


if __name__ == "__main__":
    my_ftp = SftpClient('47.253.49.46', 21, 'cxzx_750822',
                        '7075f0e4e5a41b79977e9fcf74716d57')
    my_ftp.list_dir('da')
    # my_ftp.download_file('/Users/copperfield/Downloads/da/analyst 2021-10-01.LOG', 'da/analyst 2021-10-01.LOG')
    # my_ftp.download_dir('/Users/copperfield/Downloads', 'upload')
