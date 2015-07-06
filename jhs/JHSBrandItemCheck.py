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
from JHSItemQ import JHSItemQ
from JHSWorker import JHSWorker

class JHSBrandItemCheck():
    '''A class of brand for check not start item info'''
    def __init__(self, m_type):
        # 队列标志
        self._obj = 'item'
        self._crawl_type = 'check'

        # mysql
        self.mysqlAccess = MysqlAccess()

        # item queue
        self.item_queue = JHSItemQ(self._obj, self._crawl_type)

        #self.work = JHSWorker()

        # 抓取开始时间
        self.begin_time = Common.now()

        # 分布式主机标志
        self.m_type = m_type

    def antPage(self):
        try:
            # 主机器需要配置redis队列
            if self.m_type == 'm':
                self.brandHourList()

            """
            # 附加信息
            a_val = (self.begin_time,)
            self.work.process(self._obj, self._crawl_type, a_val)
            """
        except Exception as e:
            Common.traceback_log()

    # 配置check抓取redis队列
    def brandHourList(self):
        # 查找需要check的列表
        # 得到需要的时间段
        val = (Common.time_s(self.begin_time),)
        print '# check crawler time:',val
        
        # 商品默认信息列表
        all_item_num = 0
        check_val_list = []
        act_items = {}
        item_results = self.mysqlAccess.selectJhsItemsNotStart(val)
        if item_results:
            for item in item_results:
                if act_items.has_key(str(item[0])):
                    act_items[str(item[0])]["items"].append(item[2:])
                else:
                    act_items[str(item[0])] = {'act_name':item[1],'items':[]}
                    act_items[str(item[0])]["items"].append(item[2:])
                all_item_num += 1
            for key in act_items.keys():
                check_val_list.append((key,act_items[key]["act_name"],act_items[key]["items"]))
        else:
            print '# not find need check items...'
            
        print '# check all item nums:',all_item_num
        print '# check all acts nums:',len(check_val_list)
        # 清空check抓取redis队列
        self.item_queue.clearItemQ()
        # 保存check抓取redis队列
        self.item_queue.putItemlistQ(check_val_list)
        print '# item queue end:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

if __name__ == '__main__':
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    args = sys.argv
    #args = ['JHSBrandItemCheck','m']
    if len(args) < 2:
        print '#err not enough args for JHSBrandItemCheck...'
        exit()
    # 是否是分布式中主机
    m_type = args[1]
    b = JHSBrandItemCheck(m_type)
    b.antPage()
    time.sleep(5)
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

