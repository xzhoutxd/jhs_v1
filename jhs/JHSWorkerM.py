#-*- coding:utf-8 -*-
#!/usr/bin/env python
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import time
from JHSWorker import JHSWorker
sys.path.append('../base')
import Common as Common
from ProcessMgr import ProcessMgr

class JHSWorkerM(ProcessMgr):
    '''A class of JHSWorker process manager'''
    def __init__(self, _process_num=10):
        # worker instance
        self.worker = JHSWorker()

        # parent construct
        ProcessMgr.__init__(self, _process_num, self.worker)

if __name__ == '__main__':
    # 抓取开始时间
    begin_time = Common.now()
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    args = sys.argv
    if len(args) < 4:
        print '#err not enough args for JHSWorkerM...'
        print args
        exit()
    # 处理输入参数
    p_num = int(args[1])
    obj = args[2]
    crawl_type = args[3]
    # JHS worker 多进程对象实例
    m = JHSWorkerM(p_num)
    # 多进程并发处理
    # 附加的信息
    a_val = None
    if obj == 'item':
        #if crawl_type == 'update' or crawl_type == 'day' or crawl_type == 'hour' or crawl_type == 'check':
        if crawl_type in ['update','day','hour','check']:
            a_val = (begin_time,)
    m.createProcess((obj, crawl_type, a_val))
    m.run()
    time.sleep(5)
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

