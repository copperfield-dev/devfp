import pandas as pd
import os
from hdfs.ext.kerberos import Client
from krbcontext import krbcontext

if __name__ == '__main__':
    # date_list = pd.date_range('2021-07-14', periods=7)
    # print(date_list)
    
    keytab_file = 'resource/ywmy.keytab'
    principal = 'ywmy@LZ.DSCC.99.COM'
    
    active_str = 'kinit -kt {0} {1}'.format(keytab_file, principal)
    os.system(active_str)
    
    with krbcontext(using_keytab=True, keytab_file=keytab_file, principal=principal):
        client = Client(url='https://bigdata-test', root='/user/ywmy')
        client.status('/')
        # hdfs_file_pa