import argparse
import json
import logging

from sftp_client import SftpClient

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,  # 控制台打印的日志级别
        filename='ftp.log',
        filemode='a',  ##模式，有w和a，w就是写模式，每次都会重新写日志，覆盖之前的日志
        # a是追加模式，默认如果不写的话，就是追加模式
        format=
        '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
        # 日志格式
    )
    parser = argparse.ArgumentParser()
    parser.add_argument("-v",
                        "--version",
                        action='version',
                        version='%(prog)s version : v 1.0.0',
                        help='show the version')
    parser.add_argument("-c",
                        "--conf",
                        type=str,
                        default='./ftp_clients/configure.json')
    parser.add_argument("-f", "--filename", type=str)
    args = parser.parse_args()

    config_filename = args.conf
    logging.info("FTP server config file is: " + config_filename)
    filename = args.filename
    logging.info("Download file is: " + filename)
    configure_file = open(config_filename)
    configure_data = configure_file.read()
    configure_file.close()
    ftp_configure = json.loads(configure_data)

    client = SftpClient(ftp_configure["ftp_host"], 21,
                        ftp_configure["ftp_username"],
                        ftp_configure["ftp_password"])
    client.download_file(
        ftp_configure["ftp_local_dir"] + "/" + ftp_configure["ftp_host"] + "/" + filename, filename)
