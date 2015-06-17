#-*- coding:utf-8 -*-
#!/usr/bin/env python
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import time
import random
import traceback
import threading
import base.Config as Config
import base.Common as Common
from dial.DialClient import DialClient
from base.MyThread  import MyThread
from Queue import Empty
from db.MysqlAccess import MysqlAccess
from JHSAct import JHSAct
sys.path.append('../db')
from MongofsAccess import MongofsAccess

import warnings
warnings.filterwarnings("ignore")

class JHSActM(MyThread):
    '''A class of jhs activity item thread manager'''
    def __init__(self, jhs_type, thread_num = 15, a_val=None):
        # parent construct
        MyThread.__init__(self, thread_num)

        # thread lock
        self.mutex      = threading.Lock()

        # db
        self.mysqlAccess = MysqlAccess() # mysql access
        self.mongofsAccess = MongofsAccess() # mongodb fs access

        # appendix val
        self.a_val = a_val

        # jhs queue type
        self.jhs_type   = jhs_type # 1:即将上线品牌团频道页, 2:检查每天还没结束的活动, 3:新增活动
        
        # activity items
        self.items      = []

        # dial client
        self.dial_client = DialClient()

        # local ip
        self._ip = Common.local_ip()

        # router tag
        self._tag = 'ikuai'
        #self._tag = 'tpent'

    # To dial router
    def dialRouter(self, _type, _obj):
        try:
            _module = '%s_%s' %(_type, _obj)
            self.dial_client.send((_module, self._ip, self._tag))
        except Exception as e:
            print '# To dial router exception :', e

    def push_back(self, L, v):
        if self.mutex.acquire(1):
            #self.items.append(v)
            L.append(v)
            self.mutex.release()

    def putItem(self, _item):
        self.put_q((0, _item))

    def putItems(self, _items):
        for _item in _items: self.put_q((0, _item))

    # To crawl retry
    def crawlRetry(self, _data):
        if not _data: return
        _retry, _val = _data
        _retry += 1
        if _retry < Config.act_crawl_retry:
            _data = (_retry, _val)
            self.put_q(_data)
        else:
            print "# retry too many times, no get item:", _val

    # insert act
    def insertAct(self, actsql_list, f=False):
        if f or len(actsql_list) >= Config.act_max_arg:
            if len(actsql_list) > 0:
                self.mysqlAccess.insertJhsAct(actsql_list)
            return True
        return False

    # insert act day
    def insertActday(self, actdaysql_list, f=False):
        if f or len(actdaysql_list) >= Config.act_max_arg:
            if len(actdaysql_list) > 0:   
                self.mysqlAccess.insertJhsActDayalive(actdaysql_list)
            return True
        return False

    # insert act hour
    def insertActhour(self, acthoursql_list, f=False):
        if f or len(acthoursql_list) >= Config.act_max_arg:
            if len(acthoursql_list) > 0:
                self.mysqlAccess.insertJhsActHouralive(acthoursql_list)
            return True
        return False

    # insert act coming
    def insertActcoming(self, actcomingsql_list, f=False):
        if f or len(actcomingsql_list) >= Config.act_max_arg:
            if len(actcomingsql_list) > 0:
                self.mysqlAccess.insertJhsActComing(actcomingsql_list)
            return True
        return False

    # To crawl item
    def crawl(self):
        # sql list
        #_actsql_list, _actdaysql_list, _acthoursql_list = [], [], []
        _actcomingsql_list = []
        while True:
            _data = None
            try:
                try:
                    # 取队列消息
                    _data = self.get_q()
                except Empty as e:
                    # 队列为空，退出
                    #print '# queue is empty', e
                    self.insertActcoming(_actcomingsql_list, True)
                    _actcomingsql_list = []

                    break

                item = None
                crawl_type = ''
                if self.jhs_type == 1:
                    # 品牌团实例 即将上线
                    item = JHSAct()

                    # 信息处理
                    _val  = _data[1]
                    item.antPageComing(_val)
                    print '# To crawl coming activity val : ', Common.now_s(), _val[1], _val[2], _val[3]
                    crawl_type = 'coming'
                    # 汇聚
                    self.push_back(self.items, item.outTupleForComing())
                    crawling_confirm,sql = item.outTupleForComing()
                    # 入库
                    if crawling_confirm == 1:
                        _actcomingsql_list.append(sql)
                    if self.insertActcoming(_actcomingsql_list): _actcomingsql_list = []
                elif self.jhs_type == 2:
                    # 品牌团实例 检查活动新加商品
                    item = JHSAct()

                    # 信息处理
                    _val  = _data[1]
                    item.antPageHourcheck(_val)
                    #print '# To check activity val : ', Common.now_s(), _val[0], _val[1]
                    crawl_type = 'hourcheck'
                    # 汇聚
                    self.push_back(self.items, item.outTupleForHourcheck())
                elif self.jhs_type == 3:
                    # 品牌团实例
                    item = JHSAct()

                    # 信息处理
                    _val  = _data[1]
                    item.antPage(_val)
                    #print '# To crawl activity val : ', Common.now_s(), _val[1], _val[2], _val[3]
                    crawl_type = 'brand'

                    # 汇聚
                    self.push_back(self.items, item.outTuple())

                elif self.jhs_type == 4:
                    # 还没有开团的品牌团实例
                    item = JHSAct()

                    # 信息处理
                    _val  = _data[1]
                    item.antPageMain(_val)
                    #print '# To crawl activity val : ', Common.now_s(), _val[1], _val[2], _val[3]
                    crawl_type = 'main'

                    # 汇聚
                    self.push_back(self.items, item.outTuple())

                elif self.jhs_type == 5:
                    # 解析品牌团数据
                    item = JHSAct()

                    # 信息处理
                    _val  = _data[1]
                    item.antPageParser(_val)
                    #print '# To crawl activity val : ', Common.now_s(), _val[1], _val[2], _val[3]
                    crawl_type = 'parser'

                    # 汇聚
                    self.push_back(self.items, item.outTupleParse())
                else:
                    self.queue.task_done()
                    continue

                # 存网页
                if item and crawl_type != '':
                    _pages = item.outItemPage(crawl_type)
                    self.mongofsAccess.insertJHSPages(_pages)

                    
                # 通知queue, task结束
                self.queue.task_done()

            except Common.NoActivityException as e:
                print 'Not activity exception :', e
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
                # 重新拨号
                try:
                    self.dialRouter(4, 'chn')
                except Exception as e:
                    print '# DailClient Exception err:', e
                    time.sleep(10)
                time.sleep(random.uniform(10,30))

if __name__ == '__main__':
    pass

