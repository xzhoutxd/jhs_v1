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
from JHSWorkerM import JHSWorkerM

class JHSBrandHour():
    '''A class of brand for every hour'''
    def __init__(self, m_type):
        # 队列标志
        self._obj = 'item'
        self._crawl_type = 'hour'

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

    # 配置每小时抓取redis队列
    def brandHourList(self):
        # 查找需要每小时统计的列表
        # 得到需要的时间段
        val = (Common.add_hours(self.begin_time), Common.add_hours(self.begin_time, -1))
        print '# hour crawler time:',val
        
        # 商品默认信息列表
        all_item_num = 0
        hour_val_list = []
        act_items = {}
        item_results = self.mysqlAccess.selectJhsItemsHouralive(val)
        if item_results:
            for item in item_results:
                if act_items.has_key(str(item[0])):
                    act_items[str(item[0])]["items"].append(item[2:])
                else:
                    act_items[str(item[0])] = {'act_name':item[1],'items':[]}
                    act_items[str(item[0])]["items"].append(item[2:])
                all_item_num += 1
            for key in act_items.keys():
                hour_val_list.append((key,act_items[key]["act_name"],act_items[key]["items"]))
        else:
            print '# not find need hour items...'
            
        print '# hour all item nums:',all_item_num
        print '# hour all acts nums:',len(hour_val_list)
        # 清空每小时抓取redis队列
        self.item_queue.clearItemQ()
        # 保存每小时抓取redis队列
        self.item_queue.putItemlistQ(hour_val_list)
        print '# item queue end:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

if __name__ == '__main__':
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    args = sys.argv
    #args = ['JHSBrandHour','m']
    if len(args) < 2:
        print '#err not enough args for JHSBrandHour...'
        exit()
    # 是否是分布式中主机
    m_type = args[1]
    b = JHSBrandHour(m_type)
    b.antPage()
    time.sleep(5)
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

