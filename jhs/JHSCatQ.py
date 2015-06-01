#-*- coding:utf-8 -*-
#!/usr/bin/env python
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import re
import random
import json
import time
import traceback
import base.Common as Common
import base.Config as Config
from db.MysqlAccess import MysqlAccess
from Message import Message
sys.path.append('../db')
from RedisQueue  import RedisQueue
from RedisAccess import RedisAccess

class JHSCatQ():
    '''A class of jhs cat redis queue'''
    def __init__(self):
        # DB
        self.jhs_type    = Config.JHS_TYPE   # queue type
        self.mysqlAccess = MysqlAccess() # mysql access
        self.redisQueue  = RedisQueue()      # redis queue
        self.redisAccess = RedisAccess()     # redis db

        # message
        self.message = Message()

        # giveup items
        self.giveup_items = []

    # clear cat queue
    def clearCatQ(self):
        _key = '%s_cat' % (self.jhs_type)
        self.redisQueue.clear_q(_key)

    # 写入redis queue
    def putCatQ(self, _msg):
        _key = '%s_cat' % (self.jhs_type)
        self.redisQueue.put_q(_key, _msg)

    # 转换msg
    def putCatlistQ(self, cat_list):
        _obj = 'cat'
        for _cat in cat_list:
            _val = (0,_obj,self.jhs_type) + _cat
            msg = self.message.jhsCatMsg(_val)
            self.putCatQ(msg)

