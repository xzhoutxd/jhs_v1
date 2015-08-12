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
from Message import Message
sys.path.append('../base')
import Common as Common
import Config as Config
sys.path.append('../db')
from RedisQueue  import RedisQueue
from RedisAccess import RedisAccess

class JHSCatQ():
    '''A class of jhs cat redis queue'''
    def __init__(self, _q_type):
        self._obj        = 'cat'
        self._q_type     = _q_type           # queue type
        self.jhs_type    = Config.JHS_TYPE   # obj type
        # DB
        self.redisQueue  = RedisQueue()      # redis queue
        #self.redisAccess = RedisAccess()     # redis db

        # message
        self.message     = Message()

        # queue key
        self._key        = '%s_%s_%s' % (self.jhs_type, self._obj, self._q_type)


    # clear cat queue
    def clearCatQ(self):
        self.redisQueue.clear_q(self._key)

    # 写入redis queue
    def putCatQ(self, _msg):
        self.redisQueue.put_q(self._key, _msg)

    # 转换msg
    def putCatlistQ(self, cat_list):
        for _cat in cat_list:
            _val = (0,self._obj,self.jhs_type) + _cat
            msg = self.message.jhsCatQueueMsg(_val)
            self.putCatQ(msg)

