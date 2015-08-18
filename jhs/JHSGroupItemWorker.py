#-*- coding:utf-8 -*-
#!/usr/bin/env python
import sys

import re
import random
import json
import time
import traceback
from Jsonpage import Jsonpage
from Message import Message
from JHSGroupItemM import JHSGroupItemParserM
from JHSGroupItemM import JHSGroupItemCrawlerM
from JHSGroupItemM import JHSGroupItemQM
sys.path.append('../base')
import Common as Common
import Config as Config
from TBCrawler import TBCrawler
sys.path.append('../dial')
from DialClient import DialClient
sys.path.append('../db')
from MysqlAccess import MysqlAccess
from RedisAccess import RedisAccess
from MongofsAccess import MongofsAccess
from RedisQueue  import RedisQueue

import warnings
warnings.filterwarnings("ignore")

class JHSGroupItemWorker():
    '''A class of JHS group item channel worker'''
    def __init__(self):
        # jhs group item type
        self.worker_type    = Config.JHS_GroupItem

        self.jhs_type       = Config.JHS_TYPE   # queue type

        # message
        self.message        = Message()

        # 获取Json数据
        self.jsonpage       = Jsonpage()

        # 抓取设置
        self.crawler        = TBCrawler()

        # 抓取时间设定
        self.crawling_time  = Common.now() # 当前爬取时间
        self.begin_time     = Common.now()
        self.begin_date     = Common.today_s()
        self.begin_hour     = Common.nowhour_s()

        # DB
        # mysql access
        self.mysqlAccess    = MysqlAccess()

        # redis queue
        self.redisQueue     = RedisQueue()

        # redis access
        self.redisAccess    = RedisAccess()

        # mongodb fs access
        self.mongofsAccess  = MongofsAccess()

    def init_crawl(self, _obj, _crawl_type):
        self._obj          = _obj
        self._crawl_type   = _crawl_type

        # dial client
        self.dial_client   = DialClient()

        # local ip
        self._ip           = Common.local_ip()

        # router tag
        self._router_tag   = 'ikuai'
        #self._router_tag  = 'tpent'

        # items
        self.items         = []

        # giveup items
        self.giveup_items  = []

        # giveup msg val
        self.giveup_val    = None

    # To dial router
    def dialRouter(self, _type, _obj):
        try:
            _module = '%s_%s' %(_type, _obj)
            self.dial_client.send((_module, self._ip, self._router_tag))
        except Exception as e:
            print '# To dial router exception :', e

    def push_back_list(self, L, v):
        L.extend(v)

    def push_back_val(self, L, v):
        L.append(v)

    # To crawl retry
    def crawlRetry(self, _key, msg):
        if not msg: return
        msg['retry'] += 1
        _retry = msg['retry']
        _obj = msg["obj"]
        max_time = Config.crawl_retry
        if _obj == 'groupitemcat':
            max_time = Config.json_crawl_retry
        elif _obj == 'groupitem':
            max_time = Config.item_crawl_retry
        if _retry < max_time:
            self.redisQueue.put_q(_key, msg)
        else:
            #self.push_back(self.giveup_items, msg)
            print "# retry too many time, no get:", msg

    def crawlPage(self, _key, msg, _val):
        try:
            if self._obj == 'groupitemcat':
                self.run_category(msg, _val)
            else:
                print '# crawlPage unknown obj = %s' % self._obj
        except Common.InvalidPageException as e:
            print '# Invalid page exception:',e
            self.crawlRetry(_key,msg)
        except Common.DenypageException as e:
            print '# Deny page exception:',e
            self.crawlRetry(_key,msg)
            # 重新拨号
            try:
                self.dialRouter(4, 'chn')
            except Exception as e:
                print '# DailClient Exception err:', e
                time.sleep(random.uniform(10,30))
            time.sleep(random.uniform(10,30))
        except Common.SystemBusyException as e:
            print '# System busy exception:',e
            self.crawlRetry(_key,msg)
            time.sleep(random.uniform(10,30))
        except Common.RetryException as e:
            print '# Retry exception:',e
            if self.giveup_val:
                msg['val'] = self.giveup_val
            self.crawlRetry(_key,msg)
            time.sleep(random.uniform(20,30))
        except Exception as e:
            print '# exception err:',e
            self.crawlRetry(_key,msg)
            # 重新拨号
            if str(e).find('Read timed out') == -1:
                try:
                    self.dialRouter(4, 'chn')
                except Exception as e:
                    print '# DailClient Exception err:', e
            time.sleep(random.uniform(10,30))
            Common.traceback_log()

    def run_category(self, msg, _val):
        category_val = msg["val"]
        refers = _val
        c_url,c_name,c_id = category_val
        print c_url,c_name,c_id
        page = self.crawler.getData(c_url, refers)
        page_val = (page,c_name,c_id)
        ajax_url_list = self.getAjaxurlList(page_val,c_url)
        if len(ajax_url_list) > 0:
            self.get_jsonitems(ajax_url_list)

    # get json ajax url
    def getAjaxurlList(self, page_val, refers=''):
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

    # get item json list in category page from ajax url
    def get_jsonitems(self, ajax_url_list):
        # today all items val
        todayall_item_val = []
        # other sub nav items val
        item_list = []
        # process ajax url list
        item_json_index = 0
        # mongo json pages
        cat_pages = {}
        for a_url in ajax_url_list:
            # get json from ajax url
            Result_list = self.jsonpage.get_json([a_url])
            # mongo page json
            _url,_refers,_val = a_url 
            _c_id = _val[0]
            time_s = time.strftime("%Y%m%d%H", time.localtime(self.crawling_time))
            # timeStr_jhstype_webtype_itemgroupcat_catid
            key = '%s_%s_%s_%s_%s' % (time_s,Config.JHS_TYPE,'1','itemgroupcat',str(_c_id))
            cat_pages[key] = '<!-- url=%s --> %s' % (_url,str(Result_list))

            if Result_list and len(Result_list) > 0:
                item_result_valList = self.jsonpage.parser_itemjson(Result_list)
                if len(item_result_valList) > 0:
                    item_json_index += 1
                    # the first item list is all online items
                    if item_json_index == 1:
                        if len(item_result_valList) > 0:
                            print '# all online items.....'
                            todayall_item_val = item_result_valList
                    else:
                        self.push_back_list(item_list, item_result_valList)
        if len(item_list) > 0:
            self.parseItems(item_list)

        # cat pages json 
        for key in cat_pages.keys():
            _pages = (key,cat_pages[key])
            self.mongofsAccess.insertJHSPages(_pages)

    # 解析从接口中获取的商品数据
    def parseItems(self, item_list):
        print '# parse Group Items start:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

        # 附加信息
        a_val = (self.begin_time,)
        # 多线程 控制并发的线程数
        max_th = Config.item_max_th
        if len(item_list) > max_th:
            m_itemsObj = JHSGroupItemParserM(self._crawl_type, max_th, a_val)
        else:
            m_itemsObj = JHSGroupItemParserM(self._crawl_type, len(item_list), a_val)
        m_itemsObj.createthread()
        m_itemsObj.putItems(item_list)
        m_itemsObj.run()

        _items = m_itemsObj.items
        self.push_back_list(self.items,_items)
        print '# queue item num:',len(self.items)
        print '# parse item num:',len(_items)
        print '# parse Group Items end:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

    def process(self, _obj, _crawl_type, _val=None):
        self.init_crawl(_obj, _crawl_type)
        if _obj == 'groupitem':
            self.processMulti(_val)
        else:
            self.processOne(_val)

    def processOne(self, _val=None):
        i, M = 0, 10
        n = 0
        while True: 
            try:
                if self._crawl_type and self._crawl_type != '':
                    _key = '%s_%s_%s' % (self.jhs_type, self._obj, self._crawl_type)
                else:
                    _key = '%s_%s' % (self.jhs_type, self._obj)
                _msg = self.redisQueue.get_q(_key)

                # 队列为空
                if not _msg:
                    i += 1
                    if i > M:
                        print '# all get catQ item num:',n
                        print '# not get catQ of key:',_key
                        break
                    time.sleep(10)
                    continue
                n += 1
                self.crawlPage(_key, _msg, _val)

            except Exception as e:
                print '# exception err in process of JHSGroupItemWorker:',e,_key,_msg

    def processMulti(self, _val=None):
        if self._crawl_type and self._crawl_type != '':
            _key = '%s_%s_%s' % (self.jhs_type, self._obj, self._crawl_type)
        else:
            _key = '%s_%s' % (self.jhs_type, self._obj)

        try:
            self.crawlPageMulti(_key, _val)
        except Exception as e:
            print '# exception err in processMulti of JHSGroupItemWorker: %s, key: %s' % (e,_key)

    # To crawl page
    def crawlPageMulti(self, _key, _val):
        if self._obj == 'groupitem':
            self.run_groupitem(_key, _val)
        else:
            print '# crawlPageMulti unknown obj = %s' % self._obj

    def run_groupitem(self, _key, _val):
        m_itemQ = JHSGroupItemQM(self._obj, self._crawl_type, 20, _val)
        m_itemQ.createthread()
        m_itemQ.run()
        item_list = m_itemQ.items
        print '# crawl Items num: %d' % len(item_list)

    # 删除redis数据库过期商品
    def delItem(self, _items):
        for _item in _items:
            keys = [self.worker_type, _item["item_juId"]]
            
            item = self.redisAccess.read_jhsitem(keys)
            if item:
                end_time = item["end_time"]
                now_time = Common.time_s(self.begin_time)
                # 删除过期的商品
                if now_time > end_time: self.redisAccess.delete_jhsitem(keys)

    # 把商品信息存入redis数据库中
    def putItemDB(self, _items):
        for _item in _items:
            # 忽略已经存在的商品ID
            keys = [self.worker_type, _item["item_juId"]]
            if self.redisAccess.exist_jhsitem(keys): continue

            # 将商品基础数据写入redis
            item_val = self.message.itemInfo(_item["r_val"])
            val = self.message.itemMsg(item_val)
            self.redisAccess.write_jhsitem(keys, val)

    # 更新商品信息
    def updateItem(self, _item):
        keys = [self.worker_type, _item["item_juId"]]

        item = self.redisAccess.read_jhsitem(keys)
        if item:
            item_val = self.message.itemParseInfo(_item["r_val"])
            c = False
            if item["start_time"] != item_val["start_time"]:
                item["start_time"] = item_val["start_time"]
                c = True
            if item["end_time"] != item_val["end_time"]:
                item["end_time"] = item_val["end_time"]
                c = True
            if c:
                self.redisAccess.write_jhsitem(keys, item)

    # 查找新商品
    def selectNewItems(self, _items):
        new_items = []
        for _item in _items:
            keys = [self.worker_type, _item["item_juId"]]
            if self.redisAccess.exist_jhsitem(keys): 
                self.updateItem(_item)
                continue
            new_items.append(_item["val"])
        return new_items

    def scanEndItems(self):
        val = (Common.time_s(self.crawling_time),)
        _items = self.mysqlAccess.selectJhsGroupItemEnd(val)
        end_items = []
        # 遍历商品
        for _item in _items:
            item_juid = _item[0]
            end_items.append({"item_juId":str(item_juid)})
        print '# del item nums:',len(end_items)
        # 删除已经结束的商品
        self.delItem(end_items)
            
    def scanAliveItems(self):
        # 到结束时间后的一个小时
        val = (Common.time_s(self.crawling_time), Common.add_hours(self.crawling_time, -1))
        # 查找已经开团但是没有结束的商品
        _items = self.mysqlAccess.selectJhsGroupItemAlive(val)
        print "# hour all item nums:",len(_items)
        return _items

    def scanNotEndItems(self):
        val = (Common.time_s(self.crawling_time),)
        # 查找没有结束的商品
        _items = self.mysqlAccess.selectJhsGroupItemNotEnd(val)
        i = 1
        for _item in _items:
            print i
            item_juid = str(_item[1])
            keys = [self.worker_type, item_juid]

            item = self.redisAccess.read_jhsitem(keys)
            print item
            #_new_item = {"crawling_time":item["crawling_time"],"item_juid":item["item_juId"],"groupcat_id":item["item_groupCatId"],"groupcat_name":item["item_groupCatName"],"item_ju_url":item["item_ju_url"],"item_juname":item["item_juName"],"item_id":item["item_id"],"start_time":item["start_time"],"end_time":item["end_time"]}
            #self.redisAccess.write_jhsitem(keys, _new_item)
            i += 1

    def scanCategories(self):
        category_list = self.mysqlAccess.selectJhsGroupItemCategory()
        return category_list
if __name__ == '__main__':
    pass
    #w = JHSGroupItemWorker()
    #w.scanEndItems()
    #w.scanNotEndItems()
        
