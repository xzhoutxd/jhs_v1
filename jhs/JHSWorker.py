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
from dial.DialClient import DialClient
from db.MysqlAccess import MysqlAccess
from Message import Message
from Jsonpage import Jsonpage
from JHSBActItem import JHSBActItem
from JHSItemM import JHSItemM
sys.path.append('../db')
from RedisQueue  import RedisQueue
from RedisAccess import RedisAccess
from MongofsAccess import MongofsAccess

class JHSWorker():
    '''A class of jhs worker'''
    def __init__(self):
        # jhs brand type
        self.worker_type   = Config.JHS_Brand
        # DB
        self.jhs_type      = Config.JHS_TYPE   # queue type
        self.mysqlAccess   = MysqlAccess()     # mysql access
        self.redisQueue    = RedisQueue()      # redis queue
        self.redisAccess   = RedisAccess()     # redis db
        self.mongofsAccess = MongofsAccess()   # mongodb fs access

        # 获取Json数据
        self.jsonpage      = Jsonpage()

        # message
        self.message       = Message()

        # 抓取时间设定
        self.crawling_time = Common.now() # 当前爬取时间
        self.begin_time = Common.now()
        self.begin_date = Common.today_s()
        self.begin_hour = Common.nowhour_s()

    def init_crawl(self, _obj, _crawl_type):
        self._obj = _obj
        self._crawl_type = _crawl_type

        # dial client
        self.dial_client   = DialClient()

        # local ip
        self._ip           = Common.local_ip()

        # router tag
        self._router_tag   = 'ikuai'
        #self._router_tag  = 'tpent'

        # items
        self.items = []

        # giveup items
        self.giveup_items  = []

    # To dial router
    def dialRouter(self, _type, _obj):
        try:
            _module = '%s_%s' %(_type, _obj)
            self.dial_client.send((_module, self._ip, self._router_tag))
        except Exception as e:
            print '# To dial router exception :', e

    # To crawl retry
    def crawlRetry(self, _key, msg):
        if not msg: return
        msg['retry'] += 1
        _retry = msg['retry']
        _obj = msg["obj"]
        max_time = Config.crawl_retry
        if _obj == 'cat':
            max_time = Config.json_crawl_retry
        elif _obj == 'act':
            max_time = Config.act_crawl_retry
        elif _obj == 'item':
            max_time = Config.item_crawl_retry
        if _retry < max_time:
            self.redisQueue.put_q(_key, msg)
        else:
            #self.push_back(self.giveup_items, msg)
            print "# retry too many time, no get:", msg

     # To crawl page
    def crawlPage(self, _obj, _crawl_type, _key, msg, _val):
        try:
            if _obj == 'cat':
                self.run_cat(msg, _val)
            elif _obj == 'act':
                self.run_act(msg)
            elif _obj == 'item':
                self.run_item(msg, _val)
            else:
                print '# crawlPage unknown obj = %s' % _obj
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
        except Exception as e:
            print '# exception err:',e
            self.crawlRetry(_key,msg)
            time.sleep(random.uniform(10,30))
            Common.traceback_log()

    # CAT queue
    def run_cat(self, msg, _val):
        bResult_list = []
        c_url  = msg["url"]
        a_val  = (msg["id"],msg["name"])
        refers = msg["refers"]
        print '# category',msg["id"],msg["name"]
        # get json
        bResult_list = self.jsonpage.get_jsonPage(c_url,refers,a_val)

        # parse json
        if bResult_list and bResult_list != []:
            act_valList = self.jsonpage.parser_brandjson(bResult_list,_val)
            if act_valList != []:
                print '# get brand act num:',len(act_valList)
                #self.items.extend([act_valList[0]])
                self.items.extend(act_valList)
            else:
                print '# err: not get brandjson parse val list.'

    # ACT queue
    def run_act(self, msg):
        # 默认数据
        msg_val = msg["val"]
        print '# act start:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        act_obj = None
        if self._crawl_type == 'main':
            act_obj = JHSBActItem()
            act_obj.antPageMain(msg_val)
        elif self._crawl_type == 'check':
            act_obj = JHSBActItem()
            act_obj.antPageCheck(msg_val)
        print '# act end:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

        act_keys = [self.worker_type, str(act_obj.brandact_id)]
        prev_act = self.redisAccess.read_jhsact(act_keys)

        # 是否需要抓取商品
        if act_obj.crawling_confirm != 2:
            # 只取非俪人购商品
            if act_obj and int(act_obj.brandact_sign) != 3:
                # 多线程抓商品
                self.run_actItems(act_obj, prev_act)
            else:
                print '# ladygo activity id:%s name:%s'%(act_obj.brandact_id, act_obj.brandact_name)

            print '# pro act start:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            # 处理活动信息
            self.procAct(act_obj, prev_act)
            print '# pro act end:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            
            #act_obj.crawling_confirm == 0: update item position
        else:
           print '# Already start activity, id:%s name:%s'%(act_obj.brandact_id, act_obj.brandact_name) 

    # 并行获取品牌团商品
    def run_actItems(self, act, prev_act):
        print '# act items start:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        # 需要抓取的item
        item_val_list = []
        # 过滤已经抓取过的商品ID列表
        item_ids = act.brandact_itemids
        if prev_act:
            prev_item_ids = prev_act["item_ids"]
            item_ids      = Common.diffSet(item_ids, prev_item_ids)

            # 如果已经抓取过的活动没有新上线商品，则退出
            if len(item_ids) == 0:
                print '# Activity no new Items'
                print '# Activity Items end:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), act.brandact_id, act.brandact_name
                return None

            for item in act.brandact_itemVal_list:
                if str(item[6]) in item_ids or str(item[7]) in item_ids:
                    item_val_list.append(item)
        else:
            item_val_list = act.brandact_itemVal_list

        # 如果活动没有商品, 则退出
        if len(item_ids) == 0:
            print '# run_brandItems: no items in activity, act_id=%s, act_name=%s' % (act.brandact_id,act.brandact_name)
            return None

        print '# Activity Items crawler start:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), act.brandact_id, act.brandact_name
        # 多线程 控制并发的线程数
        if len(item_val_list) > Config.item_max_th:
            m_itemsObj = JHSItemM(self._crawl_type, Config.item_max_th)
        else: 
            m_itemsObj = JHSItemM(self._crawl_type, len(item_val_list))
        m_itemsObj.createthread()
        m_itemsObj.putItems(item_val_list)
        m_itemsObj.run()

        item_list = m_itemsObj.items
        print '# Activity find new Items num:', len(item_val_list)
        print '# Activity crawl Items num:', len(item_list)
        print '# Activity Items end:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), act.brandact_id, act.brandact_name

    # To merge activity
    def mergeAct(self, act, prev_act):
        if prev_act:
            # 合并本次和上次抓取的商品ID列表
            prev_item_ids  = prev_act["item_ids"]
            act.brandact_itemids   = Common.unionSet(act.brandact_itemids, prev_item_ids)

            # 取第一次的活动抓取时间
            act.crawling_time_s = prev_act["crawl_time"]

            if not act.brandact_name or act.brandact_name == '':
                act.brandact_name = prev_act["act_name"]
            if not act.brandact_url or act.brandact_url == '':
                act.brandact_url = prev_act["act_url"]
            if not act.brandact_position or str(act.brandact_position) == '0':
                act.brandact_position = prev_act["act_position"]
            if not act.brandact_enterpic_url or act.brandact_enterpic_url == '':
                act.brandact_enterpic_url = prev_act["act_enterpic_url"]
            if not act.brandact_remindNum or str(act.brandact_remindNum) == '0':
                act.brandact_remindNum = prev_act["act_remindnum"]
            if not act.brandact_coupons or act.brandact_coupons == []:
                act.brandact_coupon = prev_act["act_coupon"]
                act.brandact_coupons = prev_act["act_coupons"].split(Config.sep)
            if not act.brandact_starttime or act.brandact_starttime == 0.0: 
                act.brandact_starttime = Common.str2timestamp(prev_act["start_time"])
            if not act.brandact_endtime or act.brandact_endtime == 0.0:
                act.brandact_endtime = Common.str2timestamp(prev_act["end_time"])
            if not act.brandact_other_ids or act.brandact_other_ids == '':
                act.brandact_other_ids = prev_act["_act_ids"]

    # To put act db
    def putActDB(self, act, prev_act):
        # redis
        self.mergeAct(act, prev_act)
        keys = [self.worker_type, str(act.brandact_id)]
        val = act.outTupleForRedis()
        self.redisAccess.write_jhsact(keys, val)
        
        if self._crawl_type == 'main':
            # mysql
            if prev_act:
                print '# update activity, id:%s name:%s'%(act.brandact_id, act.brandact_name)
                self.mysqlAccess.updateJhsAct(act.outSqlForUpdate())
            else:
                print '# insert activity, id:%s name:%s'%(act.brandact_id, act.brandact_name)
                self.mysqlAccess.insertJhsAct(act.outSql())
            # 预热信息
            self.mysqlAccess.insertJhsActComing(act.outSqlForComing())

        # mongo
        # 存网页
        #_pages = act.outItemPage(self._crawl_type)
        #self.mongofsAccess.insertJHSPages(_pages)

    # To process activity
    def procAct(self, act, prev_act):
        # 将抓取的活动信息存入redis
        self.putActDB(act, prev_act)

    # ITEM queue
    def run_item(self, msg, _val):
        # 默认数据
        msg_val = msg["val"]
        brandact_id, brandact_name, item_val_list = msg_val
        print '# Activity Items start:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), brandact_id, brandact_name
        # 多线程 控制并发的线程数
        max_th = Config.item_max_th
        if len(item_val_list) > max_th:
            m_itemsObj = JHSItemM(self._crawl_type, max_th, _val)
        else:
            m_itemsObj = JHSItemM(self._crawl_type, len(item_val_list), _val)
        m_itemsObj.createthread()
        m_itemsObj.putItems(item_val_list)
        m_itemsObj.run()

        item_list = m_itemsObj.items
        print '# Activity Items num:', len(item_val_list)
        print '# Activity crawl Items num:', len(item_list)
        print '# Activity Items end:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), brandact_id, brandact_name

    def process(self, _obj, _crawl_type, _val=None):
        self.init_crawl(_obj, _crawl_type)

        i, M = 0, 3
        n = 0
        while True: 
            if _crawl_type and _crawl_type != '':
                _key = '%s_%s_%s' % (self.jhs_type,_obj,_crawl_type)
            else:
                _key = '%s_%s' % (self.jhs_type,_obj)
            _msg = self.redisQueue.get_q(_key)

            # 队列为空
            if not _msg:
                i += 1
                if i > M:
                    print '# not get queue of key:',_key
                    print '# all get num of item in queue:',n
                    break
                time.sleep(10)
                continue
            n += 1
            try:
                self.crawlPage(_obj, _crawl_type, _key, _msg, _val)
            except Exception as e:
                print '# exception err in process of JHSWorker:',e,_key,_msg

    # 删除redis数据库过期活动
    def delAct(self, _acts):
        for _act in _acts:
            keys = [self.worker_type, str(_act["act_id"])]

            item = self.redisAccess.read_jhsact(keys)
            if item:
                end_time = item["end_time"]
                now_time = Common.time_s(self.crawling_time)
                # 删除过期的商品
                if now_time > end_time: self.redisAccess.delete_jhsact(keys)

    # 查找结束的活动
    def scanEndActs(self, val):
        _acts = self.mysqlAccess.selectJhsActEnd(val)
        end_acts = []
        for _act in _acts:
            act_id = _act[1]
            end_acts.append({"act_id":str(act_id)})
        # 删除已经结束的活动
        self.delAct(end_acts)
        return _acts

    # acts redis
    def actsRedis(self):
        i = 0
        _acts = self.mysqlAccess.selectActsRedisdata()
        print '# acts num:',len(_acts)
        for _act in _acts:
            act_id = _act[2]
            _itemids = self.mysqlAccess.selectItemsids(str(act_id))
            item_ids = []
            for _itemid in _itemids:
                item_ids.append(str(_itemid[0]))
                item_ids.append(str(_itemid[1]))
            act_val = _act + (item_ids,)
            #print act_val
            keys = [self.worker_type, str(act_id)]
            #print keys
            #if self.redisAccess.exist_jhsact(keys):
                #i += 1
                #print self.redisAccess.read_jhsact(keys)
                #self.redisAccess.delete_jhsact(keys)
            #self.redisAccess.write_jhsact(keys, act_val)
            #i += 1
            #break
        print '# redis acts num:',i

    # items redis
    def itemsRedis(self):
        _items = self.mysqlAccess.selectItemRedisdata()
        print '# items num:', len(_items)
        i = 0
        for _item in _items:
            msg = self.message.jhsitemMsg(_item)
            #print msg
            keys = [self.worker_type, str(_item[0])]
            #print keys
            if self.redisAccess.exist_jhsitem(keys):
                #print self.redisAccess.read_jhsitem(keys)
                self.redisAccess.delete_jhsitem(keys)
            self.redisAccess.write_jhsitem(keys, msg)
            i += 1 
            #break
        print '# redis items num:',i
        
            

if __name__ == '__main__':
    w = JHSWorker()
    #w.actsRedis()
    w.itemsRedis()

