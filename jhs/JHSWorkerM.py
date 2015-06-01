#-*- coding:utf-8 -*-
#!/usr/bin/env python
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from base.ProcessMgr import ProcessMgr
from JHSWorker import JHSWorker

class JHSWorkerM(ProcessMgr):
    '''A class of JHSWorker process manager'''
    def __init__(self, _process_num=10):
        # worker instance
        self.worker = JHSWorker()

        # parent construct
        ProcessMgr.__init__(self, _process_num, self.worker)

