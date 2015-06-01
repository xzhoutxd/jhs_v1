#-*- coding:utf-8 -*-
#!/usr/bin/env python

from sys import path

import json
import pickle
from RedisPool import RedisPool
path.append(r'../base')
import Common as Common

@Common.singleton
class RedisAccess:
    def __init__(self):
        # redis db instance
        self.redis_pool = RedisPool()

        # redis db id
        self.DEFAULT_DB    = 0   # default db
        
        self.TB_SHOP_DB    = 1   # taobao shop
        self.TB_ITEM_DB    = 2   # taobao item

        self.VIP_ACT_DB    = 3   # vip activity        
        self.VIP_ITEM_DB   = 4   # vip item
        self.VIP_SKU_DB    = 5   # vip sku

        self.JHS_ACT_DB    = 20  # jhs activity        
        self.JHS_ITEM_DB   = 21  # jhs item
        self.JHS_SKU_DB    = 22  # jhs SKU

        self.COOKIE_DB     = 9   # taobao cookie
        self.QUEUE_DB      = 10  # queue db

    ######################## Cookie部分 ########################

    # 判断是否存在cookie
    def exist_cookie(self, keys):
        return self.redis_pool.exists(keys, self.COOKIE_DB)

    # 删除cookie
    def remove_cookie(self, keys):
        return self.redis_pool.remove(keys, self.COOKIE_DB)

    # 查询cookie
    def read_cookie(self, keys):
        try:
            val = self.redis_pool.read(keys, self.COOKIE_DB)
            if val:
                cookie_dict = pickle.loads(val)
                _time  = cookie_dict["time"]            
                _cookie= cookie_dict["cookie"]
                return (_time, _cookie)
        except Exception, e:
            print '# Redis access read cookie exception:', e
            return None

    # 写入cookie
    def write_cookie(self, keys, val):
        try:
            _time, _cookie = val
            cookie_dict = {}
            cookie_dict["time"]   = _time
            cookie_dict["cookie"] = _cookie
            cookie_json = pickle.dumps(cookie_dict)
            
            self.redis_pool.write(keys, cookie_json, self.COOKIE_DB)
        except Exception, e:
            print '# Redis access write cookie exception:', e

    # 扫描cookie
    def scan_cookie(self):
        try:
            cookie_list = []
            cookies = self.redis_pool.scan_db(self.COOKIE_DB)
            for cookie in cookies:
                val = cookie[1]
                if val:
                    cookie_dict = pickle.loads(val)
                    _time   = cookie_dict["time"]   
                    _cookie = cookie_dict["cookie"]
                    cookie_list.append((_time, _cookie))
            return cookie_list
        except Exception, e:
            print '# Redis access scan cookie exception:', e
            return None

    ######################## JHS Activity ###################
    # 判断是否存在JHS活动
    def exist_jhsact(self, keys):
        return self.redis_pool.exists(keys, self.JHS_ACT_DB)

    # 删除jhs活动
    def delete_jhsact(self, keys):
        self.redis_pool.remove(keys, self.JHS_ACT_DB)

    # 查询jhs活动
    def read_jhsact(self, keys):
        try:
            val = self.redis_pool.read(keys, self.JHS_ACT_DB)
            return json.loads(val) if val else None
        except Exception, e:
            print '# Redis access read jhs activity exception:', e
            return None

    # 写入jhs活动
    def write_jhsact(self, keys, val):
        try:
            crawl_time, act_category, act_id, act_name, act_url, start_time, end_time, item_ids = val
            act_dict = {}
            act_dict["crawl_time"]   = crawl_time
            act_dict["act_category"] = act_category
            act_dict["act_id"]       = act_id
            act_dict["act_name"]     = act_name
            act_dict["act_url"]      = act_url
            act_dict["start_time"]   = start_time
            act_dict["end_time"]     = end_time
            act_dict["item_ids"]     = item_ids
            act_json = json.dumps(act_dict)
            self.redis_pool.write(keys, act_json, self.JHS_ACT_DB)
        except Exception, e:
            print '# Redis access write jhs activity exception:', e

    # 扫描jhs活动 - 性能不好
    def scan_jhsact(self):
        try:
            for act in self.redis_pool.scan_db(self.JHS_ACT_DB):
                key, val = act
                if not val: continue
                act_dict       = json.loads(val)
                print "# scan_jhsact %s:" %key, act_dict
        except Exception, e:
            print '# Redis access scan jhs activity exception:', e

    ######################## JHS ITEM ###################

    # 判断是否存在jhs item
    def exist_jhsitem(self, keys):
        return self.redis_pool.exists(keys, self.JHS_ITEM_DB)

    # 删除jhs item
    def delete_jhsitem(self, keys):
        self.redis_pool.remove(keys, self.JHS_ITEM_DB)

    # 查询jhs item
    def read_jhsitem(self, keys):
        try:
            val = self.redis_pool.read(keys, self.JHS_ITEM_DB)
            return json.loads(val) if val else None 
        except Exception, e:
            print '# Redis access read jhs item exception:', e
            return None

    # 写入jhs item
    def write_jhsitem(self, keys, val):
        try:
            item_json = json.dumps(val)
            self.redis_pool.write(keys, item_json, self.JHS_ITEM_DB)
        except Exception, e:
            print '# Redis access write jhs item exception:', e

    # 扫描jhs item - 性能不好
    def scan_jhsitem(self):
        try:
            for item in self.redis_pool.scan_db(self.JHS_ITEM_DB):
                key, val = item
                if not val: continue
                item_dict    = json.loads(val)
                print "# scan_jhsitem %s:" %key, item_dict
        except Exception as e:
             print '# Redis access scan jhs item exception:', e


if __name__ == '__main__1':
    r = RedisAccess()
    cookies = r.scan_cookie()
    for cookie in cookies: print cookie[0]
    r = None

if __name__ == '__main__':
    r = RedisAccess()
    r.scan_vipact()
    #r.scan_vipsku()
    


