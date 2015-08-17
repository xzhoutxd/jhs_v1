#-*- coding:utf-8 -*-
#!/usr/bin/env python
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import time
import random
import traceback
import threading
from Queue import Empty
from Message import Message
from JHSItem import JHSItem
sys.path.append('../base')
import Common as Common
import Config as Config
from MyThread  import MyThread
sys.path.append('../dial')
from DialClient import DialClient
sys.path.append('../db')
from MysqlAccess import MysqlAccess
from RedisQueue  import RedisQueue
from RedisAccess import RedisAccess
from MongofsAccess import MongofsAccess

import warnings
warnings.filterwarnings("ignore")

class JHSItemM(MyThread):
    '''A class of jhs item thread manager'''
    def __init__(self, _q_type, thread_num=10, a_val=None):
        # parent construct
        MyThread.__init__(self, thread_num)

        # thread lock
        self.mutex          = threading.Lock()

        self.worker_type    = Config.JHS_Brand

        # message
        self.message        = Message()

        # db
        self.mysqlAccess    = MysqlAccess() # mysql access
        self.redisAccess    = RedisAccess()     # redis db
        self.mongofsAccess  = MongofsAccess() # mongodb fs access

        # jhs queue type
        self._q_type        = _q_type # main:新增商品, day:每天一次的商品, hour:每小时一次的商品, update:更新

        # appendix val
        self.a_val          = a_val
        
        # activity items
        self.items          = []

        # dial client
        self.dial_client    = DialClient()

        # local ip
        self._ip            = Common.local_ip()

        # router tag
        self._tag           = 'ikuai'
        #self._tag          = 'tpent'

        # give up item, retry too many times
        self.giveup_items   = []

    # To dial router
    def dialRouter(self, _type, _obj):
        try:
            _module = '%s_%s' %(_type, _obj)
            self.dial_client.send((_module, self._ip, self._tag))
        except Exception as e:
            print '# To dial router exception :', e

    def push_back(self, L, v):
        if self.mutex.acquire(1):
            L.append(v)
            self.mutex.release()

    def putItem(self, _item):
        self.put_q((0, _item))

    def putItems(self, _items):
        for _item in _items: self.put_q((0, _item))

    # To merge item
    def mergeAct(self, item, prev_item):
        if prev_item:
            if not item.item_position or item.item_position == 0:
                item.item_position      = prev_item["item_position"]
            if not item.item_juName or item.item_juName == '':
                item.item_juName        = prev_item["item_juname"]
            if not item.item_juDesc or item.item_juDesc == '':
                item.item_juDesc        = prev_item["item_judesc"]
            if not item.item_juPic_url or item.item_juPic_url == '':
                item.item_juPic_url     = prev_item["item_jupic_url"]
            if not item.item_url or item.item_url == '':
                item.item_url           = prev_item["item_url"]
            if not item.item_oriPrice or item.item_oriPrice == '':
                item.item_oriPrice      = prev_item["item_oriprice"]
            if not item.item_actPrice or item.item_actPrice == '':
                item.item_actPrice      = prev_item["item_actprice"]
            if not item.item_discount or item.item_discount == '':
                item.item_discount      = prev_item["item_discount"]
            if not item.item_coupons or item.item_coupons == []:
                item.item_coupons       = prev_item["item_coupons"].split(Config.sep)
            if not item.item_promotions or item.item_promotions == []:
                item.item_promotions    = prev_item["item_promotions"].split(Config.sep)
            if not item.item_remindNum or item.item_remindNum == '':
                item.item_remindNum     = prev_item["item_remindnum"]
            if not item.item_isLock_time or item.item_isLock_time == '':
                if prev_item["item_islock_time"] and prev_item["item_islock_time"] != '':
                    item.item_isLock_time   = Common.str2timestamp(prev_item["item_islock_time"])
                    item.item_isLock        = prev_item["item_islock"]
            if not item.item_starttime or item.item_starttime == 0.0:
                if prev_item["start_time"] and prev_item["start_time"] != '':
                    item.item_starttime     = Common.str2timestamp(prev_item["start_time"])
            if not item.item_endtime or item.item_endtime == 0.0:
                if prev_item["end_time"] and prev_item["end_time"] != '':
                    item.item_endtime       = Common.str2timestamp(prev_item["end_time"])

    # To put item redis db
    def putItemDB(self, item):
        # redis
        keys = [self.worker_type, str(item.item_juId)]
        prev_item = self.redisAccess.read_jhsitem(keys)
        self.mergeAct(item, prev_item)
        val = item.outTupleForRedis()
        msg = self.message.jhsitemMsg(val)
        self.redisAccess.write_jhsitem(keys, msg)

    # To crawl retry
    def crawlRetry(self, _data):
        if not _data: return
        _retry, _val = _data
        _retry += 1
        if _retry < Config.item_crawl_retry:
            _data = (_retry, _val)
            self.put_q(_data)
        else:
            self.push_back(self.giveup_items, _val)
            print "# retry too many times, no get item:", _val

    # insert item info
    def insertIteminfo(self, iteminfosql_list, f=False):
        if f or len(iteminfosql_list) >= Config.item_max_arg:
            if len(iteminfosql_list) > 0:
                self.mysqlAccess.insertJhsItemInfo(iteminfosql_list)
                #print '# insert data to database'
            return True
        return False

    # insert item day
    def insertItemday(self, itemdaysql_list, f=False):
        if f or len(itemdaysql_list) >= Config.item_max_arg:
            if len(itemdaysql_list) > 0:
                self.mysqlAccess.insertJhsItemForDay(itemdaysql_list)
                #print '# day insert data to database'
            return True
        return False

    # insert item hour
    def insertItemhour(self, itemhoursql_list, f=False):
        if f or len(itemhoursql_list) >= Config.item_max_arg:
            if len(itemhoursql_list) > 0:
                self.mysqlAccess.insertJhsItemForHour(itemhoursql_list)
                #print '# hour insert data to database'
            return True
        return False

    # update item lock start-end time
    def updateItemLockStartEndtime(self, itemsql):
        if itemsql:
            self.mysqlAccess.updateJhsItemLockStartEndtime(itemsql)
            #print '# update data to database'

    def updateItems(self, itemsql_list, f=False):
        if f or len(itemsql_list) >= Config.item_max_arg:
            if len(itemsql_list) > 0:
                self.mysqlAccess.updateJhsItems(itemsql_list)
                #print '# update data to database'
            return True
        return False

    # To crawl item
    def crawl(self):
        # item sql list
        _iteminfosql_list = []
        _itemdaysql_list = []
        _itemhoursql_list = []
        _itemupdatesql_list = []
        while True:
            _data = None
            try:
                try:
                    # 取队列消息
                    _data = self.get_q()
                except Empty as e:
                    # 队列为空，退出
                    #print '# queue is empty', e
                    # info
                    self.insertIteminfo(_iteminfosql_list, True)
                    _iteminfosql_list = []

                    # day
                    self.insertItemday(_itemdaysql_list, True)
                    _itemdaysql_list = []

                    # hour
                    self.insertItemhour(_itemhoursql_list, True)
                    _itemhoursql_list = []

                    # update
                    #self.updateItems(_itemupdatesql_list, True)
                    #_itemupdatesql_list = []

                    break

                item = None
                if self._q_type == 'main':
                    # 新商品实例
                    item = JHSItem()
                    _val = _data[1]
                    if self.a_val: _val = _val + self.a_val
                    item.antPage(_val)
                    # 汇聚
                    # redis
                    self.putItemDB(item)
                    self.push_back(self.items, item.outTuple())
                    # 入库
                    iteminfoSql = item.outTuple()
                    _iteminfosql_list.append(iteminfoSql)
                    if self.insertIteminfo(_iteminfosql_list): _iteminfosql_list = []
                elif self._q_type == 'day':
                    # 每天商品实例
                    item = JHSItem()
                    _val = _data[1]
                    if self.a_val: _val = _val + self.a_val
                    item.antPageDay(_val)
                    # 汇聚
                    self.push_back(self.items, item.outSqlForDay())
                    # 入库
                    daySql = item.outSqlForDay()
                    _itemdaysql_list.append(daySql)
                    if self.insertItemday(_itemdaysql_list): _itemdaysql_list = []
                elif self._q_type == 'hour':
                    # 每小时商品实例
                    item = JHSItem()
                    _val = _data[1]
                    if self.a_val: _val = _val + self.a_val
                    item.antPageHour(_val)
                    # 汇聚
                    # redis
                    self.putItemDB(item)
                    self.push_back(self.items, item.outTupleHour())
                    # 入库
                    updateSql = item.outSqlForUpdate()
                    if updateSql:
                        self.mysqlAccess.updateJhsItem(updateSql)

                    hourSql = item.outSqlForHour()
                    _itemhoursql_list.append(hourSql)
                    if self.insertItemhour(_itemhoursql_list): _itemhoursql_list = []
                elif self._q_type == 'update':
                    # 更新商品
                    item = JHSItem()
                    _val = _data[1]
                    if self.a_val: _val = _val + self.a_val

                    item.antPageUpdate(_val)
                    # 汇聚
                    # redis
                    self.putItemDB(item)
                    self.push_back(self.items, item.outSqlForUpdate())
                    # 入库
                    updateSql = item.outSqlForUpdate()
                    if updateSql:
                        self.mysqlAccess.updateJhsItem(updateSql)
                elif self._q_type == 'check':
                    # check商品实例
                    item = JHSItem()
                    _val = _data[1]
                    if self.a_val: _val = _val + self.a_val
                    item.antPageUpdate(_val)
                    # 汇聚
                    # redis
                    self.putItemDB(item)
                    self.push_back(self.items, item.outSqlForUpdate())
                    # 入库
                    updateSql = item.outSqlForUpdate()
                    if updateSql:
                        self.mysqlAccess.updateJhsItem(updateSql)

                # 存网页
                if item:
                    _pages = item.outItemPage(self._q_type)
                    self.mongofsAccess.insertJHSPages(_pages)

                # 延时
                time.sleep(1)
                # 通知queue, task结束
                self.queue.task_done()

            except Common.NoItemException as e:
                print 'Not item exception :', e
                # 通知queue, task结束
                self.queue.task_done()

            except Common.NoPageException as e:
                print 'Not page exception :', e
                # 通知queue, task结束
                self.queue.task_done()

            except Common.InvalidPageException as e:
                self.crawlRetry(_data)
                print 'Invalid page exception :', e
                # 通知queue, task结束
                self.queue.task_done()

            except Exception as e:
                print 'Unknown exception crawl item :', e
                Common.traceback_log()
                self.crawlRetry(_data)
                # 通知queue, task结束
                self.queue.task_done()
                #if str(e).find('Name or service not known') != -1 or str(e).find('Temporary failure in name resolution') != -1:
                #    print _data
                # 重新拨号
                if str(e).find('Read timed out') == -1:
                    try:
                        self.dialRouter(4, 'item')
                    except Exception as e:
                        print '# DailClient Exception err:', e 
                        time.sleep(10)
                time.sleep(random.uniform(10,40))



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
                    crawl_type = 'grouphour'
                    # 汇聚
                    #self.push_back(self.items, item.outTupleGroupItemHour())

                    # 入库
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

                    # 入库
                    iteminfoSql = item.outTupleGroupItem()
                    _iteminfosql_list.append(iteminfoSql)
                    if self.insertIteminfo(_iteminfosql_list): _iteminfosql_list = []
                else:
                    continue

                # 存网页
                if item and crawl_type != '':
                    _pages = item.outItemPage(crawl_type)
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
                Common.traceback_log()

                self.crawlRetry(_data)
                #if str(e).find('Name or service not known') != -1 or str(e).find('Temporary failure in name resolution') != -1:
                #    print _data
                # 重新拨号
                if str(e).find('Read timed out') == -1:
                    try:
                        self.dialRouter(4, 'item')
                    except Exception as e:
                        print '# DailClient Exception err:', e
                        time.sleep(10)
                time.sleep(random.uniform(10,30))



