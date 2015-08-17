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
from JHSWorker import JHSWorker
sys.path.append('../base')
import Common as Common
import Config as Config
from RetryCrawler import RetryCrawler
sys.path.append('../db')
from MysqlAccess import MysqlAccess

class JHSBrandPosition():
    '''A class of JHS brand act position'''
    def __init__(self, m_type):
        # 抓取设置
        self.crawler = RetryCrawler()

        # DB
        self.mysqlAccess   = MysqlAccess()     # mysql access

        # cat homeposition queue
        self.home_queue = JHSQ('cat', 'homeposition')

        # cat position queue
        self.cat_queue = JHSQ('cat','position')

        # act queue
        self.act_queue = JHSQ('act','position')

        self.work = JHSWorker()

        # 默认类别
        #self.category_list = [("http://ju.taobao.com/jusp/nvzhuangpindao/tp.htm#J_FixedNav","女装","1000")]
        self.category_list = [
                ("http://ju.taobao.com/jusp/nvzhuangpindao/tp.htm#J_FixedNav","女装","1000"),
                ("http://ju.taobao.com/jusp/nanzhuangpindao/tp.htm#J_FixedNav","男装","7000"),
                ("http://ju.taobao.com/jusp/xiebaopindao/tp.htm#J_FixedNav","鞋包","3000"),
                ("http://ju.taobao.com/jusp/neiyipindao/tp.htm#J_FixedNav","内衣","4000"),
                ("http://ju.taobao.com/jusp/zhubaoshipin/tp.htm#J_FixedNav","饰品","42000"),
                ("http://ju.taobao.com/jusp/yundongpindao/tp.htm#J_FixedNav","运动","38000"),
                ("http://ju.taobao.com/jusp/meizhuangpindao/tp.htm#J_FixedNav","美妆","2000"),
                ("http://ju.taobao.com/jusp/tongzhuangpindao/tp.htm#J_FixedNav","童装","23000"),
                ("http://ju.taobao.com/jusp/shipinpindao/tp.htm#J_FixedNav","零食","5000"),
                ("http://ju.taobao.com/jusp/muyingpindao/tp.htm#J_FixedNav","母婴","6000"),
                ("http://ju.taobao.com/jusp/baihuopindao/tp.htm#J_FixedNav","百货","37000"),
                ("http://ju.taobao.com/jusp/chepinpindao/tp.htm#J_FixedNav","汽车","36000"),
                ("http://ju.taobao.com/jusp/jiadianpindao/tp.htm#J_FixedNav","家电","34000"),
                ("http://ju.taobao.com/jusp/shumapindao/tp.htm#J_FixedNav","数码","43000"),
                ("http://ju.taobao.com/jusp/jiajunewpindao/tp.htm#J_FixedNav","家装","225000"),
                ("http://ju.taobao.com/jusp/jiajupindao/tp.htm#J_FixedNav","家纺","35000")
                ]

        # 页面
        self.site_page  = None

        # 抓取开始时间
        self.begin_time = Common.now()

        # 分布式主机标志
        self.m_type = m_type

    def antPage(self):
        try:
            # 主机器需要配置redis队列
            if self.m_type == 'm':
                # 清空分类类表也home url redis队列
                self.home_queue.clearQ()
                # 保存到redis队列
                self.home_queue.putlistQ([(Config.ju_brand_home, Config.ju_home)])
                print '# cat homeposition queue end:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

                # 商品团分类页面
                category_list = self.mysqlAccess.selectJhsGroupItemCategory()
                if not category_list or len(category_list) == 0:
                    category_list = self.category_list
                if category_list and len(category_list) > 0:
                    cate_val_list = []
                    for cate in category_list:
                        cate_val_list.append((cate[0],cate[2],cate[1],Config.ju_home_today,Config.JHS_GroupItem))
                    # 清空category redis队列
                    self.cat_queue.clearQ()
                    # 保存category redis队列
                    self.cat_queue.putlistQ(cate_val_list)

                    # 清空act redis队列
                    self.act_queue.clearQ()
                    print '# category position queue end:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                else:
                    print '# not find category...'

            # 类目json url list
            obj = 'cat'
            crawl_type = 'homeposition'
            self.work.process(obj,crawl_type)
            # json url list
            json_val_list = self.work.items
            if json_val_list and len(json_val_list) > 0:
                # 保存到redis队列
                self.cat_queue.putlistQ(json_val_list)
                print '# cat position queue end:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

            # 类目的活动Json
            obj = 'cat'
            crawl_type = 'position'
            # 获取还没有开团的活动id
            a_val = (self.begin_time,)
            self.work.process(obj,crawl_type,a_val)

            # 活动数据
            act_val_list = self.work.items
            print '# act nums:', len(act_val_list)

            # 保存到redis队列
            self.act_queue.putlistQ(act_val_list)
            print '# act queue end:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

        except Exception as e:
            print '# antpage error :',e
            Common.traceback_log()

if __name__ == '__main__':
    args = sys.argv
    #args = ['JHSBrandPosition','m']
    if len(args) < 2:
        print '#err not enough args for JHSBrandPosition...'
        exit()
    # 是否是分布式中主机
    m_type = args[1]
    j = JHSBrandPosition(m_type)
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    j.antPage()
    time.sleep(1)
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
