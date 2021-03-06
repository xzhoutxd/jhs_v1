#-*- coding:utf-8 -*-
#!/usr/bin/env python
from sys import path

import MySQLdb
from DBUtils.PooledDB import PooledDB
path.append(r'../base')
import Environ as Environ

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

g_jhsDbPool = MysqlPool(Environ.mysql_config['jhs'])

