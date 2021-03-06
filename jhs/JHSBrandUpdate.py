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
from JHSWorkerM import JHSWorkerM
sys.path.append('../base')
import Common as Common
import Config as Config
sys.path.append('../db')
from MysqlAccess import MysqlAccess

class JHSBrandUpdate():
    '''A class of brand update'''
    def __init__(self, m_type):
        # 队列标志
        self._obj = 'item'
        self._crawl_type = 'update'

        # DB
        self.mysqlAccess = MysqlAccess()     # mysql access

        # item queue
        self.item_queue = JHSQ(self._obj, self._crawl_type)

        # 抓取开始时间
        self.begin_time = Common.now()

        # 即将开团的最小时间
        self.min_hourslot = 1 # 最小时间段

        # 分布式主机标志
        self.m_type = m_type

    def antPage(self):
        try:
            # 更新即将开团活动的商品信息
            # 主机器需要配置redis队列
            if self.m_type == 'm':
                # 一个小时即将开团
                val = (Common.time_s(self.begin_time),Common.add_hours(self.begin_time, self.min_hourslot))
                print '# update time:',val

                # 商品默认信息列表
                all_item_num = 0
                update_val_list = []
                act_items = {}
                item_results = self.mysqlAccess.selectJhsItemsForUpdate(val)
                if item_results:
                    for item in item_results:
                        if act_items.has_key(str(item[0])):
                            act_items[str(item[0])]["items"].append(item[2:]) 
                        else:
                            act_items[str(item[0])] = {'act_name':item[1],'items':[]}
                            act_items[str(item[0])]["items"].append(item[2:])
                        all_item_num += 1
                    for key in act_items.keys():
                        update_val_list.append((key,act_items[key]["act_name"],act_items[key]["items"]))
                else:
                    print '# not find need update items...'
                print '# need update all items nums:',all_item_num
                print '# need update all acts nums:',len(update_val_list)

                # 清空redis队列
                self.item_queue.clearQ()
                # 保存到redis队列
                self.item_queue.putlistQ(update_val_list)
                print '# item queue end:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

            """
            # 附加的信息
            a_val = (self.begin_time,)
            self.work.process(self._obj, self._crawl_type, a_val)
            """
            
        except Exception as e:
            print '# exception err in antPage info:',e
            Common.traceback_log()

if __name__ == '__main__':
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    args = sys.argv
    #args = ['JHSBrandUpdate','m|s']
    if len(args) < 2:
        print '#err not enough args for JHSBrandUpdate...'
        exit()
    # 是否是分布式中主机
    m_type = args[1]
    b = JHSBrandUpdate(m_type)
    b.antPage()
    time.sleep(5)
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))


