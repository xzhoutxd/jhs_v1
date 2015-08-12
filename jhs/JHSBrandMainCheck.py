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
from JHSActQ import JHSActQ
from JHSWorker import JHSWorker
from JHSWorkerM import JHSWorkerM
sys.path.append('../base')
import Common as Common
import Config as Config
sys.path.append('../db')
from MysqlAccess import MysqlAccess

class JHSBrandMainCheck():
    '''A class of brand check'''
    def __init__(self, m_type):
        # 队列标志
        self._obj = 'act'
        self._crawl_type = 'check'

        # act queue
        self.act_queue = JHSActQ(self._crawl_type)

        # DB
        self.mysqlAccess = MysqlAccess()     # mysql access

        #self.work = JHSWorker()

        # 抓取开始时间
        self.begin_time = Common.now()

        # 分布式主机标志
        self.m_type = m_type


    def antPage(self):
        try:
            # 主机器需要配置redis队列
            if self.m_type == 'm':
                # 获取还没结束的活动
                val = (Common.time_s(self.begin_time),)
                acts = self.mysqlAccess.selectJhsActNotEnd(val)
                if not acts or len(acts) == 0:
                    print '# Main check activity not found..'
                    return None

                # 活动信息列表
                act_val_list = []
                for act in acts:
                    act_val_list.append((str(act[1]),act[7],act[8],self.begin_time,str(act[28]),str(act[29])))
                print '# Main check activity num:',len(act_val_list)

                # 清空act redis队列
                self.act_queue.clearActQ()
                # 保存到redis队列
                self.act_queue.putActlistQ(act_val_list)
                print '# act queue end:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

            """
            #self.work.process(self._obj, self._crawl_type)
            """

        except Exception as e:
            print '# exception err in antPage info:',e
            Common.traceback_log()

if __name__ == '__main__':
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    args = sys.argv
    #args = ['JHSBrandMainCheck','m|s']
    if len(args) < 2:
        print '#err not enough args for JHSBrandMainCheck...'
        exit()
    # 是否是分布式中主机
    m_type = args[1]
    b = JHSBrandMainCheck(m_type)
    b.antPage()
    time.sleep(5)
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

