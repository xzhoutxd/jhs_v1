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
    def __init__(self,_q_type):
        self._obj        = 'act'
        self._q_type     = _q_type           # queue type
        self.jhs_type    = Config.JHS_TYPE   # obj type
        # DB
        self.redisQueue  = RedisQueue()      # redis queue
        #self.redisAccess = RedisAccess()     # redis db

        # message
        self.message     = Message()

        # queue key
        self._key = '%s_%s_%s' % (self.jhs_type, self._obj, self._q_type)

    # clear activity queue
    def clearActQ(self):
        self.redisQueue.clear_q(self._key)

    # 写入redis queue
    def putActQ(self, _msg):
        _data = _msg
        self.redisQueue.put_q(self._key, _data)

    # 转换msg
    def putActlistQ(self, brandact_list):
        for _act in brandact_list:
            _val = (0,self._obj,self.jhs_type) + _act
            msg = self.message.jhsActQueueMsg(_val)
            self.putActQ(msg)


