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
from JHSQ import JHSQ
from JHSWorker import JHSWorker
from JHSWorkerM import JHSWorkerM
sys.path.append('../base')
import Common as Common
import Config as Config
sys.path.append('../db')
from MysqlAccess import MysqlAccess

class JHSBrandDay():
    '''A class of brand for every day'''
    def __init__(self, m_type):
        # 队列标志
        self._obj = 'item'
        self._crawl_type = 'day'

        # mysql
        self.mysqlAccess = MysqlAccess()

        # item queue
        self.item_queue = JHSQ(self._obj, self._crawl_type)

        #self.work = JHSWorker()

        # 抓取开始时间
        self.begin_time = Common.now()

        # 分布式主机标志
        self.m_type = m_type

    def antPage(self):
        try:
            # 主机器需要配置redis队列
            if self.m_type == 'm':
                self.brandDayList()
            """
            # 附加信息
            a_val = (self.begin_time,)
            #self.work.process(self._obj, self._crawl_type, a_val)
            """
        except Exception as e:
            Common.traceback_log()

    # 配置redis队列
    def brandDayList(self):
        # 查找需要每天统计的活动列表
        # 当前时刻减去24小时
        val = (Common.today_s()+" 00:00:00",Common.add_hours(self.begin_time, -24))
        print '# day crawler time:',val
        # 商品默认信息列表
        all_item_num = 0
        day_val_list = []
        act_items = {}
        item_results = self.mysqlAccess.selectJhsItemsDayalive(val)
        if item_results:
            for item in item_results:
                if act_items.has_key(str(item[0])):
                    act_items[str(item[0])]["items"].append(item[2:])
                else:
                    act_items[str(item[0])] = {'act_name':item[1],'items':[]}
                    act_items[str(item[0])]["items"].append(item[2:])
                all_item_num += 1
            for key in act_items.keys():
                day_val_list.append((key,act_items[key]["act_name"],act_items[key]["items"]))
        else:
            print '# not find need day items...'
            
        print '# day all item nums:',all_item_num
        print '# need update all acts nums:',len(day_val_list)
        # 清空每天抓取redis队列
        self.item_queue.clearQ()
        # 保存每天抓取redis队列
        self.item_queue.putlistQ(day_val_list)
        print '# item queue end:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

if __name__ == '__main__':
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    args = sys.argv
    #args = ['JHSBrandDay','m|s']
    if len(args) < 2:
        print '#err not enough args for JHSBrandDay...'
        exit()
    # 是否是分布式中主机
    m_type = args[1]
    b = JHSBrandDay(m_type)
    b.antPage()
    time.sleep(5)
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

