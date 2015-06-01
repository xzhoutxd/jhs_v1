#-*- coding:utf-8 -*-
#!/usr/bin/env python

from sys import path
path.append(r'../base')

import pymongo  # 只能用2.8版本
import gridfs
import Environ as Environ

class MongoPool(object):
    '''A class of mongodb connection pool'''
    def __init__(self):
        # 数据库配置
        db_config    = Environ.mongodb_config
        _host, _port = db_config['host'], db_config['port']

        # fs前缀
        self.fs_prefix = Environ.mongodb_fs

        # mongodb connect pool
        self.mongo   = pymongo.Connection(host=_host, port=_port)

        # 淘宝/天猫网页库
        self.tb_db   = self.mongo["tb"]

        # Vip网页库
        self.vip_db  = self.mongo["vip"]

        # JHS网页库
        self.jhs_db  = self.mongo["jhs"]

        # 网页库
        self._db = None

        # fs
        self._fs = None

    def __del__(self):
         self.mongo.close()

    @property
    def db(self):
        return self._db

    @db.setter
    def db(self, db_name):
        self._db = self.mongo[db_name]

    # 创建索引
    def crtTBIndex(self, c):
        self.tb_db[c].ensure_index('key', unique=True)

    def crtVipIndex(self, c):
        self.vip_db[c].ensure_index('key', unique=True)

    def crtJHSIndex(self, c):
        self.jhs_db[c].ensure_index('key', unique=True)

    def crtIndex(self, db_name, c):
        self.db = db_name
        self.db[c].ensure_index('key', unique=True)

    # 删除表
    def dropTable(self, c):
        self.tb_db[c].drop()
        self.vip_db[c].drop()
        self.jhs_db[c].drop()

    def dropTableNew(self, db_name, c):
        self.db = db_name
        self.db[c].drop()
    
    def insertTBPage(self, c, _data):
        self.tb_db[c].insert(_data)

    def insertVipPage(self, c, _data):
        self.vip_db[c].insert(_data)

    def insertJHSPage(self, c, _data):
        self.jhs_db[c].insert(_data)

    def removeTBPage(self, c, _key):
        self.tb_db[c].remove({"key":_key})

    def removeVipPage(self, c, _key):
        self.vip_db[c].remove({"key":_key})

    def removeJHSPage(self, c, _key):
        self.jhs_db[c].remove({"key":_key})

    def findTBPage(self, c, _key):
        return self.tb_db[c].find_one({"key":_key})

    def findVipPage(self, c, _key):
        return self.vip_db[c].find_one({"key":_key})

    def findJHSPage(self, c, _key):
        return self.jhs_db[c].find_one({"key":_key})

    def findTBPages(self, c):
        return self.tb_db[c].find()

    def findVipPages(self, c):
        return self.vip_db[c].find()

    def findJHSPages(self, c):
        return self.jhs_db[c].find()

    # new
    def insertPage(self, db_name, c, _data):
        self.db = db_name
        self.db[c].insert(_data)

    def removePage(self, db_name, c, _key):
        self.db = db_name
        self.db[c].remove({"key":_key})

    def findPage(self, db_name, c, _key):
        self.db = db_name
        return self.db[c].find_one({"key":_key})

    def findPages(self, db_name, c):
        self.db = db_name
        return self.db[c].find()

    # gridfs page
    @property
    def fs(self):
        return self._fs

    @fs.setter
    def fs(self, db_name):
        self.db = db_name
        self._fs = gridfs.GridFS(self.db,self.fs_prefix)

    def putfsPage(self, db_name, _data, filename=None):
        self.fs = db_name
        f_id = self.fs.put(_data,)
        return f_id

    def getfsPage(self, db_name, _id):
        self.fs = db_name
        self.fs.get(_id)

    def findfsPage(self, db_name, _key):
        self.fs = db_name
        return self.db[c].find_one({"key":_key})

    def findfsPages(self, db_name):
        self.fs = db_name
        return self.db[c].find()

if __name__ == '__main__':
    mongo = MongoPool()
    mongo = None


