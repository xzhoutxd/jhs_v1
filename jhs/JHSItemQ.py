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
from JHSItemM import JHSItemM
from JHSGroupItemM import JHSGroupItemParserM
from JHSGroupItemM import JHSGroupItemCrawlerM
from Message import Message
sys.path.append('../base')
import Common as Common
import Config as Config
sys.path.append('../db')
from RedisQueue  import RedisQueue
from RedisAccess import RedisAccess

class JHSItemQ():
    '''A class of jhs Item redis queue'''
    def __init__(self, _obj, _q_type):
        self._obj       = _obj
        self._q_type    = _q_type           # queue type
        self.jhs_type   = Config.JHS_TYPE   # queue type
        # DB
        self.redisQueue = RedisQueue()      # redis queue

        # message
        self.message    = Message()

        # queue key
        self._key       = '%s_%s_%s' % (self.jhs_type,self._obj,self._q_type)

    # clear item queue
    def clearItemQ(self):
        self.redisQueue.clear_q(self._key)

    # 写入redis queue
    def putItemQ(self, _msg):
        _data = _msg
        self.redisQueue.put_q(self._key, _data)

    # 转换msg
    def putItemlistQ(self, item_list):
        for _item in item_list:
            _val = (0,self._obj,self.jhs_type) + _item
            msg = self.message.jhsItemQueueMsg(_val)
            self.putItemQ(msg)



import threading
from dial.DialClient import DialClient
from base.MyThread  import MyThread
from db.MysqlAccess import MysqlAccess
from JHSItem import JHSItem
from MongoAccess import MongoAccess
from MongofsAccess import MongofsAccess
import warnings
warnings.filterwarnings("ignore")

class JHSItemQM(MyThread):
    '''A class of jhs Item redis queue'''
    def __init__(self, itemtype, q_type, thread_num=10, a_val=None):
        # parent construct
        MyThread.__init__(self, thread_num)
        # thread lock
        self.mutex = threading.Lock()

        self.jhs_type    = Config.JHS_TYPE # jhs type
        self.item_type   = itemtype      # item type

        # db
        self.mysqlAccess = MysqlAccess() # mysql access
        self.redisQueue  = RedisQueue()  # redis queue
        #self.mongoAccess = MongoAccess() # mongodb access
        self.mongofsAccess = MongofsAccess() # mongodb fs access

        # jhs queue type
        self.jhs_queue_type = q_type     # h:每小时
        self._key = '%s_%s_%s' % (self.jhs_type,self.item_type,self.jhs_queue_type)

        # appendix val
        self.a_val = a_val

        # activity items
        self.items = []

        # dial client
        self.dial_client = DialClient()

        # local ip
        self._ip = Common.local_ip()

        # router tag
        self._tag = 'ikuai'
        #self._tag = 'tpent'

        # give up item, retry too many times
        self.giveup_items = []

    # To dial router
    def dialRouter(self, _type, _obj):
        try:
            _module = '%s_%s' %(_type, _obj)
            self.dial_client.send((_module, self._ip, self._tag))
        except Exception as e:
            print '# To dial router exception :', e

     # clear item queue
    def clearItemQ(self):
        self.redisQueue.clear_q(self._key)

    # 写入redis queue
    def putItemQ(self, _msg):
        _data = (0, _msg)
        self.redisQueue.put_q(self._key, _data)

    # 转换msg
    def putItemlistQ(self, item_list):
        for _item in item_list:
            #msg = self.q_message.itemMsg(_item)
            msg = _item
            self.putItemQ(msg)

    def push_back(self, L, v):
        if self.mutex.acquire(1):
            L.append(v)
            self.mutex.release()

    # To crawl retry
    def crawlRetry(self, _data):
        if not _data: return
        _retry, _val = _data
        _retry += 1
        if _retry < Config.crawl_retry:
            _data = (_retry, _val)
            self.redisQueue.put_q(self._key, _data)
        else:
            self.push_back(self.giveup_items, _val)
            print "# retry too many times, no get item:", _val

    # insert item info
    def insertIteminfo(self, iteminfosql_list, f=False):
        if f or len(iteminfosql_list) >= Config.item_max_arg:
            if len(iteminfosql_list) > 0:
                self.mysqlAccess.insertJhsGroupItemInfo(iteminfosql_list)
                #print '# insert data to database'
            return True
        return False

    # insert item hour
    def insertItemhour(self, itemhoursql_list, f=False):
        if f or len(itemhoursql_list) >= Config.item_max_arg:
            if len(itemhoursql_list) > 0:
                self.mysqlAccess.insertJhsGroupItemForHour(itemhoursql_list)
                #print '# insert hour data to database'
            return True
        return False

    # item sql list
    def crawl(self):
        _iteminfosql_list = []
        _itemhoursql_list = []
        i, M = 0, 10
        n = 0
        while True:
            try:
                _data = self.redisQueue.get_q(self._key)

                # 队列为空
                if not _data:
                    # 队列为空，退出
                    #print '# queue is empty', e
                    # info
                    self.insertIteminfo(_iteminfosql_list, True)
                    _iteminfosql_list = []

                    # hour
                    self.insertItemhour(_itemhoursql_list, True)
                    _itemhoursql_list = []

                    #print '# all get itemQ item num:',n
                    #print '# not get itemQ of key:',self._key
                    #break

                    i += 1
                    if i > M:
                        print '# all get itemQ item num:',n
                        print '# not get itemQ of key:',self._key
                        break
                    time.sleep(10)
                    continue

                n += 1
                item = None
                crawl_type = ''
                if self.jhs_queue_type == 'h':
                    # 每小时一次商品实例
                    item = JHSItem()
                    _val = _data[1]
                    if self.a_val: _val = _val + self.a_val

                    item.antPageGroupItemHour(_val)
                    #print '# Hour To crawl item val : ', Common.now_s(), _val[0], _val[4], _val[5]
                    crawl_type = 'grouphour'
                    # 汇聚
                    #self.push_back(self.items, item.outTupleGroupItemHour())

                    update_Sql,hourSql = item.outTupleGroupItemHour()
                    if update_Sql:
                        self.mysqlAccess.updateJhsGroupItem(update_Sql)
                    _itemhoursql_list.append(hourSql)
                    if self.insertItemhour(_itemhoursql_list): _itemhoursql_list = []

                elif self.jhs_queue_type == 'i':
                    # 商品信息
                    item = JHSItem()
                    _val = _data[1]
                    if self.a_val: _val = _val + self.a_val

                    item.antPageGroupItem(_val)
                    #print '# To crawl item val : ', Common.now_s(), _val[0], _val[4], _val[5]
                    crawl_type = 'groupitem'
                    # 汇聚
                    self.push_back(self.items, item.outTupleGroupItem())

                    iteminfoSql = item.outTupleGroupItem()
                    _iteminfosql_list.append(iteminfoSql)
                    if self.insertIteminfo(_iteminfosql_list): _iteminfosql_list = []
                else:
                    continue

                # 存网页
                if item and crawl_type != '':
                    _pages = item.outItemPage(crawl_type)
                #    self.mongoAccess.insertJHSPages(_pages)
                    self.mongofsAccess.insertJHSPages(_pages)

                # 延时
                time.sleep(1)

            except Common.NoItemException as e:
                print 'Not item exception :', e

            except Common.NoPageException as e:
                print 'Not page exception :', e

            except Common.InvalidPageException as e:
                self.crawlRetry(_data)
                print 'Invalid page exception :', e

            except Exception as e:
                print 'Unknown exception crawl item :', e
                #traceback.print_exc()
                print Common.traceback_log()

                self.crawlRetry(_data)
                if str(e).find('Name or service not known') != -1 or str(e).find('Temporary failure in name resolution') != -1:
                    print _data
                # 重新拨号
                try:
                    self.dialRouter(4, 'item')
                except Exception as e:
                    print '# DailClient Exception err:', e
                    time.sleep(10)
                time.sleep(random.uniform(10,30))


