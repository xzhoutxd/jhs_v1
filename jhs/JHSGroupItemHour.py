#-*- coding:utf-8 -*-
#!/usr/bin/env python
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import re
import random
import json
import time
from JHSQ import JHSQ
from Jsonpage import Jsonpage
from JHSGroupItemWorker import JHSGroupItemWorker
sys.path.append('../base')
import Common as Common
import Config as Config

class JHSGroupItemHour():
    '''A class of JHS group item hour sale'''
    def __init__(self, m_type):
        # 分布式主机标志
        self.m_type = m_type

        # 获取Json数据
        self.jsonpage = Jsonpage()

        self.worker = JHSGroupItemWorker()

        # item queue
        self.item_queue = JHSQ('groupitem', 'hour')

        # 抓取开始时间
        self.crawling_time = Common.now() # 当前爬取时间
        self.begin_time = Common.now()
        self.begin_date = Common.today_s()
        self.begin_hour = Common.nowhour_s()

    def antPage(self):
        try:
            # 主机器需要配置redis队列
            if self.m_type == 'm':
                # 获取已经开团的商品
                hour_items = self.worker.scanAliveItems()
                if hour_items and len(hour_items) > 0:
                    # 清空每小时商品redis队列
                    self.item_queue.clearQ() 
                    # 保存每小时商品redis队列
                    self.item_queue.putlistQ(hour_items)
                    print '# groupitem hour queue end:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                else:
                    print '# groupitem not find hour items...'

	    # 附加信息
            a_val = (self.begin_time, self.begin_hour)
            obj = 'groupitem'
            crawl_type = 'hour'
            self.worker.process(obj, crawl_type, a_val)
        except Exception as e:
            print '# antpage error :',e
            Common.traceback_log()

if __name__ == '__main__':
    args = sys.argv
    #args = ['JHSGroupItemHour','m']
    if len(args) < 2:
        print '#err not enough args for JHSGroupItemHour...'
        exit()
    # 是否是分布式中主机
    m_type = args[1]

    j = JHSGroupItemHour(m_type)
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    j.antPage()
    time.sleep(1)
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
