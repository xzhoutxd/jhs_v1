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
from Message import Message
sys.path.append('../db')
from RedisQueue  import RedisQueue
from RedisAccess import RedisAccess

class JHSActQ():
    '''A class of jhs act redis queue'''
    def __init__(self,_q_type=None):
        # DB
        self.jhs_type    = Config.JHS_TYPE   # obj type
        self.q_type      = _q_type           # queue type
        self.redisQueue  = RedisQueue()      # redis queue
        #self.redisAccess = RedisAccess()     # redis db

    # clear activity queue
    def clearActQ(self):
        if self.q_type:
            _key = '%s_act_%s' % (self.jhs_type, self.q_type)
        else:
             _key = '%s_act' % self.jhs_type
        self.redisQueue.clear_q(_key)

    # 写入redis queue
    def putActQ(self, _msg):
        if self.q_type:
            _key = '%s_act_%s' % (self.jhs_type, self.q_type)
        else:
            _key = '%s_act' % self.jhs_type
        _data = _msg
        self.redisQueue.put_q(_key, _data)

    # 转换msg
    def putActlistQ(self, brandact_list):
        for _act in brandact_list:
            msg = _act
            self.putActQ(msg)


