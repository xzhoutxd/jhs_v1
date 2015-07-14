#-*- coding:utf-8 -*-
#!/usr/bin/env python
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import re
import random
import json
import time
import base.Common as Common
import base.Config as Config
from base.RetryCrawler import RetryCrawler
from db.MysqlAccess import MysqlAccess
from JHSQ import JHSQ
from JHSCatQ import JHSCatQ
from JHSActQ import JHSActQ
from JHSWorker import JHSWorker

class JHSBrand():
    '''A class of JHS category channel'''
    def __init__(self, m_type):
        # 抓取设置
        self.crawler = RetryCrawler()

        # DB
        self.mysqlAccess   = MysqlAccess()     # mysql access

        # cat queue
        self.cat_queue = JHSQ('cat','main')

        # act queue
        self.act_queue = JHSQ('act','main')

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
                    print '# category queue end:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                else:
                    print '# not find category...'

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
            _val = (self.begin_time, brandact_id_list)
            self.work.process(obj,crawl_type,_val)

            # 活动数据
            act_val_list = self.work.items
            print '# act nums:', len(act_val_list)

            # 保存到redis队列
            self.act_queue.putlistQ(act_val_list)
            print '# act queue end:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

            if self.m_type == 'm':
                val = (Common.add_hours(self.begin_time, -2),Common.add_hours(self.begin_time, -2),Common.add_hours(self.begin_time, -1))
                # 删除Redis中上个小时结束的活动
                print '# end acts num:',len(_acts)
                _acts = self.mysqlAccess.selectJhsActEndLastOneHour(val)
                self.work.delAct(_acts)
                # 删除Redis中上个小时结束的商品
                _items = self.mysqlAccess.selectJhsItemEndLastOneHour(val)
                print '# end items num:',len(_items)
                self.work.delItem(_items)
        except Exception as e:
            print '# antpage error :',e
            Common.traceback_log()

    # 商品团频道
    def categoryListTEMP(self):
        page = self.crawler.getData(Config.ju_home_today, Config.ju_home)
        if not page or page == '': print '# not get today page'
        category_list = []
        m = re.search(r'<div class="J_CatLeft layout-left">.+?<table>(.+?)</table>.+?</div>',page,flags=re.S)
        if m:
            category_list = self.categoryListType1(m.group(1))
        else:
            m = re.search(r'<div class="catbg">\s+<div class="ju-wrapper">\s+<div class="cat-menu-h".+?>.+?<ul class="clearfix">(.+?)</ul>',page,flags=re.S)

            if m:
                category_list = self.categoryListType2(m.group(1))

        return category_list

    def categoryListType1(self,page):
        category_list = []
        m = re.search(r'<tr class="h2">.+?</tr>(.+?)<tr class="h2">',page,flags=re.S)
        if m:
            cate_list = m.group(1)
            p = re.compile(r'<a.+?href="(.+?)".+?>(.+?)</a>',flags=re.S)
            for cate in p.finditer(cate_list):
                category_list.append((cate.group(1),cate.group(2).strip()))
        return category_list
    
    def categoryListType2(self,page):
        category_list = []
        p = re.compile(r'<a.+?href="(.+?)".+?>(.+?)</a>',flags=re.S)
        for cate in p.finditer(page):
            category_list.append((cate.group(1),cate.group(2).strip()))
        return category_list

if __name__ == '__main__':
    args = sys.argv
    #args = ['JHSBrand','m']
    if len(args) < 2:
        print '#err not enough args for JHSBrand...'
        exit()
    # 是否是分布式中主机
    m_type = args[1]
    j = JHSBrand(m_type)
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    j.antPage()
    time.sleep(1)
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
