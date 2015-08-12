#-*- coding:utf-8 -*-
#!/usr/bin/env python

import MySQLdb
from DBUtils.PooledDB import PooledDB
#import base.Environ as Environ

class MysqlPool:
    '''A class of connect pool to Mysql Database'''
    def __init__(self, args):
        # mysql connect params
        args["charset"]    = "utf8"
        args["setsession"] =['SET AUTOCOMMIT = 1']

        # mysql connect pool
        self.pooldb = PooledDB(MySQLdb, **args)

    def __del__(self):
        if self.pooldb: self.pooldb.close()

    def select(self, sql, args=None):
        try:
            conn= self.pooldb.connection()
            cur = conn.cursor()
            cur.execute(sql, args)
            results = cur.fetchall()
            cur.close()
            conn.close()
            return results
        except Exception as e:
            print '# MyDatabase select exception :', e, sql, args
            return None

    def execute(self, sql, args=None):
        try:
            conn= self.pooldb.connection()
            cur = conn.cursor()
            cur.execute(sql, args)
            cur.close()
            conn.close()
        except Exception as e:
            print '# MyDatabase execute exception :', e, sql, args

    def executemany(self, sql, args_list=[]):
        try:
            conn= self.pooldb.connection()
            cur = conn.cursor()
            cur.executemany(sql, args_list)
            cur.close()
            conn.close()
        except Exception as e:
            print '# MyDatabase executemany exception :', e, sql, args_list

# mysql database pool
mysql_config = {
    #'jhs' : {'host':'192.168.1.113', 'user':'jhs', 'passwd':'123456',  'db':'jhs'},
    #'jhs' : {'host':'127.0.0.1', 'user':'root', 'passwd':'zhouxin',  'db':'jhs'},
    # test
    'jhs' : {'host':'127.0.0.1', 'user':'root', 'passwd':'zhouxin',  'db':'jhs_test'},
    #'jhs' : {'host':'192.168.1.112', 'user':'jhs', 'passwd':'123456',  'db':'jhs'},
}

g_jhsDbPool = MysqlPool(mysql_config['jhs'])

