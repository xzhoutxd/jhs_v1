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
from JHSBrandTEMP import JHSBrandTEMP
from JHSCatQ import JHSCatQ
from JHSActQ import JHSActQ
from JHSWorker import JHSWorker
sys.path.append('../base')
import Common as Common
import Config as Config
from RetryCrawler import RetryCrawler
sys.path.append('../db')
from MysqlAccess import MysqlAccess

class JHSBrandMain():
    '''A class of brand Main'''
    def __init__(self, m_type):
        # 抓取设置
        self.crawler = RetryCrawler()

        # DB
        self.mysqlAccess   = MysqlAccess()     # mysql access

        # 页面模板解析
        self.brand_temp = JHSBrandTEMP()

        # cat home queue
        self.home_queue = JHSCatQ('home')

        # cat queue
        self.cat_queue = JHSCatQ('main')

        # act queue
        self.act_queue = JHSActQ('main')

        self.work = JHSWorker()

        # 页面信息
        self.ju_brand_page = '' # 聚划算品牌团页面

        # 抓取开始时间
        self.begin_time = Common.now()

        # 分布式主机标志
        self.m_type = m_type

    def antPage(self):
        try:
            # 主机器需要配置redis队列
            if self.m_type == 'm':
                # 清空分类类表也home url redis队列
                self.home_queue.clearCatQ()
                # 保存到redis队列
                self.home_queue.putCatlistQ([(Config.ju_brand_home, Config.ju_home)])
                print '# cat home queue end:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                
            # 类目json url list
            obj = 'cat'
            crawl_type = 'home'
            self.work.process(obj,crawl_type)
            # json url list
            json_val_list = self.work.items
            if json_val_list and len(json_val_list) > 0:
                # 清空分类的json url redis队列
                self.cat_queue.clearCatQ()
                # 保存到redis队列
                self.cat_queue.putCatlistQ(json_val_list)
                print '# cat main queue end:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

                # 清空act redis队列
                self.act_queue.clearActQ()

            # 类目的活动Json
            obj = 'cat'
            crawl_type = 'main'
            # 获取还没有开团的活动id
            val = (Common.time_s(Common.now()),)
            acts = self.mysqlAccess.selectJhsActNotStart(val)
            brandact_id_list = []
            if acts:
                for act in acts:
                    brandact_id_list.append(str(act[1]))
            a_val = (self.begin_time, brandact_id_list)
            self.work.process(obj,crawl_type,a_val)

            # 活动数据
            act_val_list = self.work.items
            # 保存到redis队列
            self.act_queue.putActlistQ(act_val_list)
            print '# act queue end:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

            """
            #obj = 'act'
            #crawl_type = 'main'
            #self.work.process(obj,crawl_type)
            """

        except Exception as e:
            print '# exception err in antPage info:',e
            Common.traceback_log()


    def antPageOld(self):
        try:
            # 主机器需要配置redis队列
            if self.m_type == 'm':
                # 获取品牌团列表页数据
                page = self.crawler.getData(Config.ju_brand_home, Config.ju_home)
                if not page or page == '': raise Common.InvalidPageException("# main: not get JHS brand home.")
                self.ju_brand_page = page
                # 保存html文件
                #page_datepath = 'act/main/' + time.strftime("%Y/%m/%d/%H/", time.localtime(self.begin_time))
                #Config.writefile(page_datepath,'main-brand.htm',self.ju_brand_page)

                # 数据接口URL list
                c_url_val_list = self.brand_temp.temp(page)
                json_val_list = []
                for c_url_val in c_url_val_list:
                    c_url, c_name, c_id = c_url_val
                    json_val_list.append((Common.fix_url(c_url),c_name,c_id,Config.ju_brand_home))

                # 清空分类的json url redis队列
                self.cat_queue.clearCatQ()
                # 保存到redis队列
                self.cat_queue.putCatlistQ(json_val_list)
                print '# cat queue end:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

                # 清空act redis队列
                self.act_queue.clearActQ()

            # 类目的活动Json
            obj = 'cat'
            crawl_type = None
            # 获取还没有开团的活动id
            val = (Common.time_s(Common.now()),)
            acts = self.mysqlAccess.selectJhsActNotStart(val)
            brandact_id_list = []
            if acts:
                for act in acts:
                    brandact_id_list.append(str(act[1]))
            a_val = (self.begin_time, brandact_id_list)
            self.work.process(obj,crawl_type,a_val)

            # 活动数据
            act_val_list = self.work.items
            # 保存到redis队列
            self.act_queue.putActlistQ(act_val_list)
            print '# act queue end:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

            """
            #obj = 'act'
            #crawl_type = 'main'
            #self.work.process(obj,crawl_type)
            """

        except Exception as e:
            print '# exception err in antPage info:',e
            Common.traceback_log()

if __name__ == '__main__':
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    args = sys.argv
    #args = ['JHSBrandMain','m|s']
    if len(args) < 2:
        print '#err not enough args for JHSBrandMain...'
        exit()
    # 是否是分布式中主机
    m_type = args[1]
    b = JHSBrandMain(m_type)
    b.antPage()
    time.sleep(5)
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))



