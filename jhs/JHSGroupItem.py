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
from JHSGroupItemWorker import JHSGroupItemWorker
from JHSGroupItemM import JHSGroupItemCrawlerM
sys.path.append('../base')
import Common as Common
import Config as Config
from RetryCrawler import RetryCrawler

class JHSGroupItem():
    '''A class of JHS group item channel'''
    def __init__(self, m_type):
        # 分布式主机标志
        self.m_type = m_type

        # 抓取设置
        self.crawler = RetryCrawler()

        # cat queue
        self.cat_queue = JHSQ('groupitemcat', 'main')

        self.worker = JHSGroupItemWorker()

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
        self.crawling_time = Common.now() # 当前爬取时间
        self.begin_time = Common.now()
        self.begin_date = Common.today_s()
        self.begin_hour = Common.nowhour_s()

    def antPage(self):
        try:
            # 主机器需要配置redis队列
            if self.m_type == 'm':
                category_list = self.worker.scanCategories()
                if not category_list or len(category_list) == 0:
                    category_list = self.category_list
                if category_list and len(category_list) > 0:
                    # 清空category redis队列
                    self.cat_queue.clearQ()
                    # 保存category redis队列
                    self.cat_queue.putlistQ(category_list)
                    print '# groupitem category queue end:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                else:
                    print '# groupitem not find category...'

            obj = 'groupitemcat'
            crawl_type = 'main'
            self.worker.process(obj, crawl_type, Config.ju_home_today)
            items = self.worker.items
            print '# all parser items num:',len(items)
            # 查找新上商品
            self.get_newitems(items)

            if self.m_type == 'm':
                # 删除Redis中结束商品
                self.worker.scanEndItems()
        except Exception as e:
            print '# antpage error :',e
            Common.traceback_log()

    # 查找新上商品,并抓取新上商品详情
    def get_newitems(self, items):
        result_items = []
        for item in items:
            item_status, item_val, o_val = item
            item_juid = item_val[1]
            result_items.append({"item_juId":str(item_juid),"val":o_val,"r_val":item_val})
        new_item_list = self.worker.selectNewItems(result_items)
        print '# new items num:',len(new_item_list)
        # 抓取新上商品
        itemcrawl_type = 'new'
        # 附加信息
        a_val = (self.begin_time,)
        items = self.crawlNewItems(new_item_list,itemcrawl_type,a_val)

        # 保存新上商品信息到Redis
        new_items = []
        for item in items:
            iteminfoSql = item
            item_juid = iteminfoSql[1]
            new_items.append({"item_juId":item_juid,"r_val":iteminfoSql})
        self.worker.putItemDB(new_items)

    # 抓取新上的商品详情
    def crawlNewItems(self, _new_items, itemcrawl_type, a_val):
        print '# crawl Group Items start:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        # 多线程 控制并发的线程数
        max_th = Config.item_max_th
        if len(_new_items) > max_th:
            m_itemsObj = JHSGroupItemCrawlerM(itemcrawl_type, max_th, a_val)
        else:
            m_itemsObj = JHSGroupItemCrawlerM(itemcrawl_type, len(_new_items), a_val)
        m_itemsObj.createthread()
        m_itemsObj.putItems(_new_items)
        m_itemsObj.run()

        _items = m_itemsObj.items
        print '# insert new item num:',len(_items)
        print '# crawl Group Items end:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        return _items
 
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

    def getAjaxurlList(self,page_val,refers):
        url_list = []
        page, c_name, c_id = page_val
        p = re.compile(r'<.+?data-ajaxurl="(.+?)".+?>(.+?)</div>',flags=re.S)
        i = 0
        for a_info in p.finditer(page):
            c_subNav = c_name
            a_url = a_info.group(1).replace('amp;','')
            info = a_info.group(2)
            m = re.search(r'<span class="l-f-tbox">(.+?)</span>',info,flags=re.S)
            if m:
                c_subNav = m.group(1).strip()
            a_val = (c_id,c_name,refers,c_subNav)
            url_list.append((a_url,refers,a_val))
            i += 1
        return url_list


if __name__ == '__main__':
    args = sys.argv
    #args = ['JHSGroupItem','m']
    if len(args) < 2:
        print '#err not enough args for JHSGroupItem...'
        exit()
    # 是否是分布式中主机
    m_type = args[1]
    j = JHSGroupItem(m_type)
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    j.antPage()
    time.sleep(1)
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
