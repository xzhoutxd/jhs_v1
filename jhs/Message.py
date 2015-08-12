#-*- coding:utf-8 -*-
#!/usr/bin/env python
import sys
sys.path.append('../base')
import Common as Common

@Common.singleton
class Message():
    '''A class of jhs brand itemgroup message'''
    def __init__(self):
        pass

    ##### 品牌团 #####
    def jhsQueueMsg(self, _obj, _val):
        if _obj == "cat":
            return self.jhsCatQueueMsg(_val)
        elif _obj == "act":
            return self.jhsActQueueMsg(_val)
        elif _obj == "item":
            return self.jhsItemQueueMsg(_val)
        else:
            return None

    """
    def jhsCatQueueMsg(self, _cat):
        _retry, _obj, _type, _c_url, _c_id, _c_name, _refers = _cat
        cat = {}
        cat["retry"]  = _retry
        cat["obj"]    = _obj
        cat["type"]   = _type
        cat["url"]    = _c_url
        cat["id"]     = _c_id
        cat["name"]   = _c_name
        cat["refers"] = _refers
        return cat
    """

    def jhsCatQueueMsg(self, _cat):
        cat = {}
        cat["retry"]  = _cat[0]
        cat["obj"]    = _cat[1]
        cat["type"]   = _cat[2]
        cat["val"]    = _cat[3:]
        return cat

    def jhsActQueueMsg(self, _act):
        act = {}
        act["retry"]  = _act[0]
        act["obj"]    = _act[1]
        act["type"]   = _act[2]
        act["val"]    = _act[3:]
        return act

    def jhsItemQueueMsg(self, _act):
        act = {}
        act["retry"]  = _act[0]
        act["obj"]    = _act[1]
        act["type"]   = _act[2]
        act["val"]    = _act[3:]
        return act

    # 商品Redis数据
    def jhsitemMsg(self, _item):
        item_juid, item_id, item_position, item_ju_url, item_juname, item_judesc, item_jupic_url, item_url, item_oriprice, item_actprice, item_discount, item_coupons, item_promotions, item_remindnum, item_islock_time, item_islock, start_time, end_time = _item
        item = {}
        item["item_juid"]           = str(item_juid)
        item["item_id"]             = str(item_id)
        item["item_position"]       = str(item_position)
        item["item_ju_url"]         = item_ju_url
        item["item_juname"]         = item_juname
        item["item_judesc"]         = item_judesc
        item["item_jupic_url"]      = item_jupic_url
        item["item_url"]            = item_url
        item["item_oriprice"]       = str(item_oriprice)
        item["item_actprice"]       = str(item_actprice)
        item["item_discount"]       = str(item_discount)
        item["item_coupons"]        = item_coupons
        item["item_promotions"]     = item_promotions
        item["item_remindnum"]      = str(item_remindnum)
        if item_islock_time:
            item["item_islock_time"]    = str(item_islock_time)
        else:
            item["item_islock_time"]    = ''
        item["item_islock"]         = str(item_islock)
        item["start_time"]          = str(start_time)
        item["end_time"]            = str(end_time)
        return item

    ##### 商品团 #####
    # 商品解析数据
    def itemParseInfo(self, _item):
        _crawling_time,_item_juid,_groupcat_id,_groupcat_name,_subnav_name,_item_position,_item_ju_url,_item_jupic_url,_item_juname,_item_judesc,_item_id,_item_status,_item_merit,_item_oriprice,_item_actprice,_item_discount,_item_remindnum,_item_soldcount,_start_time,_end_time,_begin_date,_begin_hour = _item
        item = {}
        item["crawling_time"]  = _crawling_time
        item["item_juid"]      = _item_juid
        item["groupcat_id"]    = _groupcat_id
        item["groupcat_name"]  = _groupcat_name
        item["subnav_name"]    = _subnav_name
        item["item_position"]  = _item_position
        item["item_ju_url"]    = _item_ju_url
        item["item_jupic_url"] = _item_jupic_url
        item["item_juname"]    = _item_juname
        item["item_judesc"]    = _item_judesc
        item["item_id"]        = _item_id
        item["item_status"]    = _item_status
        item["item_merit"]     = _item_merit
        item["item_oriprice"]  = _item_oriprice
        item["item_actprice"]  = _item_actprice
        item["item_discount"]  = _item_discount
        item["item_remindnum"] = _item_remindnum
        item["item_soldcount"] = _item_soldcount
        item["start_time"]     = _start_time
        item["end_time"]       = _end_time
        item["begin_date"]     = _begin_date
        item["begin_hour"]     = _begin_hour
        return item

    # 商品详情数据
    def itemInfo(self, _item):
        _crawling_time,_item_juid,_groupcat_id,_groupcat_name,_subnav_name,_item_position,_item_ju_url,_item_jupic_url,_item_juname,_item_judesc,_item_id,_item_url,_item_status,_item_merit,_seller_id,_seller_name,_shop_type,_item_oriprice,_item_actprice,_item_discount,_item_remindnum,_item_soldcount,_item_coupons,_item_promotions,_start_time,_end_time = _item
        item = {}
        item["crawling_time"]  = _crawling_time
        item["item_juid"]      = _item_juid
        item["groupcat_id"]    = _groupcat_id
        item["groupcat_name"]  = _groupcat_name
        item["subnav_name"]    = _subnav_name
        item["item_position"]  = _item_position
        item["item_ju_url"]    = _item_ju_url
        item["item_jupic_url"] = _item_jupic_url
        item["item_juname"]    = _item_juname
        item["item_judesc"]    = _item_judesc
        item["item_id"]        = _item_id
        item["item_url"]       = _item_url
        item["item_status"]    = _item_status
        item["item_merit"]     = _item_merit
        item["seller_id"]      = _seller_id
        item["seller_name"]    = _seller_name
        item["shop_type"]      = _shop_type
        item["item_oriprice"]  = _item_oriprice
        item["item_actprice"]  = _item_actprice
        item["item_discount"]  = _item_discount
        item["item_remindnum"] = _item_remindnum
        item["item_soldcount"] = _item_soldcount
        item["start_time"]     = _start_time
        item["end_time"]       = _end_time
        return item

    # 商品redis数据
    def itemMsg(self, _item):
        item = {}
        item["crawling_time"]  = _item["crawling_time"]
        item["item_juid"]      = _item["item_juid"]
        item["groupcat_id"]    = _item["groupcat_id"]
        item["groupcat_name"]  = _item["groupcat_name"]
        item["item_ju_url"]    = _item["item_ju_url"]
        item["item_juname"]    = _item["item_juname"]
        item["item_id"]        = _item["item_id"]
        item["start_time"]     = _item["start_time"]
        item["end_time"]       = _item["end_time"]
        return item

