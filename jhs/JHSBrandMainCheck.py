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
from JHSActQ import JHSActQ
from JHSWorker import JHSWorker
from JHSWorkerM import JHSWorkerM

class JHSBrandMainCheck():
    '''A class of brand check'''
    def __init__(self, m_type):
        # act queue
        self.act_queue = JHSActQ('check')

        # DB
        self.mysqlAccess = MysqlAccess()     # mysql access

        #self.work = JHSWorker()

        # 分布式主机标志
        self.m_type = m_type

        # 抓取开始时间
        self.begin_time = Common.now()

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

            obj = 'act'
            crawl_type = 'check'
            #self.work.process(obj, crawl_type)
            # JHS worker 多进程对象实例
            p_num = 4
            m = JHSWorkerM(p_num)
            # 多进程并发处理
            m.createProcess((obj, crawl_type))
            m.run()

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
    time.sleep(1)
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

