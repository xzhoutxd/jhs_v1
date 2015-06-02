#-*- coding:utf-8 -*-
#!/usr/bin/env python
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import traceback
import MysqlPool
import base.Config as Config

class MysqlAccess():
    '''A class of mysql db access'''
    def __init__(self):
        # 聚划算
        self.jhs_db = MysqlPool.g_jhsDbPool

    def __del__(self):
        # 聚划算
        self.jhs_db = None

    # 新加活动
    def insertJhsAct(self, args):
        try:
            sql = 'replace into nd_jhs_parser_activity(crawl_time,act_id,category_id,category_name,act_position,act_platform,act_channel,act_name,act_url,act_desc,act_logopic_url,act_enterpic_url,act_status,act_sign,_act_ids,seller_id,seller_name,shop_id,shop_name,discount,act_soldcount,act_remindnum,act_coupon,act_coupons,brand_id,brand_name,juhome,juhome_position,start_time,end_time,c_begindate,c_beginhour) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            self.jhs_db.execute(sql, args)
            #self.jhs_db.executemany(sql, args_list)
        except Exception, e:
            print '# insert Jhs Act exception:', e

    # 更新活动信息
    def updateJhsAct(self, args):
        try:
            sql = 'call sp_update_jhs_parser_activity(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            self.jhs_db.execute(sql, args)
            #self.jhs_db.executemany(sql, args_list)
        except Exception, e:
            print '# update Jhs Act exception:', e

    # 新加商品信息
    def insertJhsItemInfo(self, args_list):
        try:
            sql = 'call sp_jhs_parser_item_mid_info(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            #self.jhs_db.execute(sql, args)
            self.jhs_db.executemany(sql, args_list)
        except Exception, e:
            print '# insert Jhs Item info exception:', e

    # 更新商品位置信息
    def updateJhsItemPosition(self, args_list):
        try:
            sql = 'call sp_update_item_position(%s,%s,%s,%s,%s)'
            self.jhs_db.executemany(sql, args_list)
        except Exception, e:
            print '# update Jhs Item position exception:', e

    # 更新商品信息
    def updateJhsItemLockStartEndtime(self, args):
        try:
            sql = 'call sp_update_jhs_item_lock_startendtime(%s,%s,%s,%s,%s)'
            self.jhs_db.execute(sql, args)
        except Exception, e:
            print '# update Jhs Item lock start-end time exception:', e

    # 更新商品信息
    def updateJhsItems(self, args_list):
        try:
            sql = 'call sp_update_jhs_item(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            self.jhs_db.executemany(sql, args_list)
        except Exception, e:
            print '# update Jhs Items exception:', e

    # 更新商品关注人数
    def updateJhsItemRemind(self, args_list):
        try:
            sql = 'call sp_update_item_remind(%s,%s)'
            self.jhs_db.executemany(sql, args_list)
        except Exception, e:
            print '# update Jhs Item remind exception:', e

    # 活动商品关系
    def insertJhsActItemRelation(self, args_list):
        try:
            sql = 'call sp_jhs_parser_activity_item(%s,%s,%s)'
            self.jhs_db.executemany(sql, args_list)
        except Exception, e:
            print '# insert Jhs Act Item relation exception:', e

    # 即将上线活动
    def insertJhsActComing(self, args):
        try:
            sql = 'call sp_jhs_parser_activity_coming(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            self.jhs_db.execute(sql, args)
            #self.jhs_db.executemany(sql, args_list)
        except Exception, e:
            print '# insert Jhs Act for Coming soon exception:', e

    # 按照活动Id查找商品Id
    def selectJhsItemIdsOfActId(self, args):
        try:
            #sql = 'select item_juid from nd_jhs_parser_item_info where act_id = %s'
            sql = 'select a.item_juid,b.item_id from nd_jhs_parser_activity_item a join nd_jhs_parser_item_info b on a.item_juid = b.item_juid where a.act_id = %s'
            return self.jhs_db.select(sql, args)
        except Exception, e:
            print '# select act items exception:', e

    # 执行SQL
    def executeSql(self, sql):
        try:
            self.jhs_db.execute(sql)
        except Exception, e:
            print '# execute Sql exception:', e

    # 还没有开团的活动
    def selectJhsActNotStart(self, args):
        # 非俪人购
        try:
            sql = 'select * from nd_jhs_parser_activity where start_time > %s and act_sign != 3'
            return self.jhs_db.select(sql, args)
        except Exception, e:
            print '# select Jhs not start act exception:', e

    def selectJhsActEnd(self, args):
        # 非俪人购
        try:
            sql = 'select * from nd_jhs_parser_activity where end_time < %s and act_sign != 3'
            return self.jhs_db.select(sql, args)
        except Exception, e:
            print '# select Jhs alive act exception:', e

    def selectJhsActNotEnd(self, args):
        # 非俪人购
        try:
            sql = 'select * from nd_jhs_parser_activity where end_time >= %s and act_sign != 3'
            return self.jhs_db.select(sql, args)
        except Exception, e:
            print '# select Jhs alive act exception:', e

    # 查找还没有结束的活动
    def selectJhsActAlive(self, args):
        # 非俪人购
        try:
            sql = 'select * from nd_jhs_parser_activity where start_time < %s and end_time > %s and act_sign != 3' 
            return self.jhs_db.select(sql, args)
        except Exception, e:
            print '# select Jhs alive act exception:', e

    # 查找需要更新的商品信息
    def selectJhsItemsForUpdate(self, args):
        try:
            sql = 'select b.act_id,b.act_name,a.item_juid,b.act_id,a.item_ju_url,b.act_url,a.item_id from nd_jhs_parser_item_info a join nd_jhs_parser_activity b on a.act_id = b.act_id where a.start_time > %s and a.start_time < %s'
            return self.jhs_db.select(sql, args)
        except Exception, e:
            print '# select Jhs update items exception:', e

    # 查找需要每天抓取活动 商品
    def selectJhsItemsDayalive(self, args):
        # (今天0点大于开始时间，当前时间减去24小时小于等于结束时间) 需要每天抓取
        try:
            sql = 'select b.act_id,b.act_name,a.item_juid,b.act_id,b.act_name,b.act_url,a.item_juname,a.item_ju_url,a.item_id,a.item_url,a.item_oriprice,a.item_actprice from nd_jhs_parser_item_info as a join nd_jhs_parser_activity as b on a.act_id = b.act_id where a.start_time < %s and a.end_time >= %s'
            return self.jhs_db.select(sql, args)
        except Exception, e:
            print '# select Jhs alive act items for day exception:', e

    # 查找需要每小时抓取活动 商品
    def selectJhsItemsHouralive(self, args):
        # (当前时间大于等于开始时间，当前时间减去1小时小于等于结束时间) 需要每小时抓取
        try:
            sql = 'select b.act_id,b.act_name,a.item_juid,a.act_id,a.item_ju_url,b.act_url,a.item_id from nd_jhs_parser_item_info as a join nd_jhs_parser_activity as b on a.act_id = b.act_id where a.start_time <= %s and a.end_time >= %s'
            return self.jhs_db.select(sql, args)
        except Exception, e:
            print '# select Jhs alive items for hour exception:', e

    # 每天抓取商品
    def insertJhsItemForDay(self, args_list):
        try:
            #sql = 'replace into nd_jhs_parser_item_d(crawl_time,item_juid,act_id,act_name,act_url,item_juname,item_ju_url,item_id,item_url,item_oriprice,item_actprice,item_soldcount,item_stock,c_begindate,c_beginhour) value(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            #self.jhs_db.execute(sql, args)
            sql = 'call sp_jhs_parser_item_d(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            self.jhs_db.executemany(sql, args_list)
        except Exception, e:
            print '# insert Jhs Item for day exception:', e
    
    # 每小时抓取商品
    def insertJhsItemForHour(self, args_list):
        try:
            sql = 'call sp_jhs_parser_item_h(%s,%s,%s,%s,%s,%s)'
            self.jhs_db.executemany(sql, args_list)
        except Exception, e:
            print '# insert Jhs Item for hour exception:', e

    # 活动位置信息
    def insertJhsActPosition(self, args_list):
        try:
            sql = 'call sp_jhs_parser_activity_position(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            self.jhs_db.executemany(sql, args_list)
        except Exception, e:
            print '# insert Jhs act position exception:', e

    def update_item_totalstock(self, args_list):
        try:
            sql = 'call sp_jhs_mid_item_totalstock(%s,%s,%s)'
            self.jhs_db.executemany(sql, args_list)
        except Exception, e:
            print '# update Jhs item totalstock exception:', e

    def selectSQL(self, sql):
        try:
            return self.jhs_db.select(sql)
        except Exception, e:
            print '# select SQL exception:', e

    # redis info data
    def selectActsRedisdata(self):
        try:
            sql = 'select crawl_time, category_id, act_id, act_name, act_url, act_position, act_enterpic_url, act_remindnum, act_coupon, act_coupons, act_sign, _act_ids, start_time, end_time from nd_jhs_parser_activity where end_time >= now()'
            return self.jhs_db.select(sql)
        except Exception, e:
            print '# select Jhs not end acts exception:', e

    def selectItemsids(self, actid):
        try:
            sql = 'select a.item_juid, b.item_id from nd_jhs_parser_activity_item a join nd_jhs_parser_item_info b on a.item_juid = b.item_juid where a.act_id = %s'
            return self.jhs_db.select(sql,str(actid))
        except Exception, e:
            print '# select Jhs acts item ids exception:', e

    def selectItemRedisdata(self):
        try:
            sql = 'select a.item_juid, a.item_id, a.item_position, a.item_ju_url, a.item_juname, a.item_judesc, a.item_jupic_url, a.item_url, a.item_oriprice, a.item_actprice, a.discount, a.item_coupons, a.item_promotions, a.item_remindnum, b.islock_time, b.islock,a.start_time, a.end_time from nd_jhs_parser_item_info a join nd_jhs_mid_item_info b on a.item_juid = b.item_juid where a.end_time >= now()'
            return self.jhs_db.select(sql)
        except Exception, e:
            print '# select Jhs not end items exception:', e

    # JHS 商品团
    # 查找商品团分类
    def selectJhsGroupItemCategory(self):
        try:
            sql = 'select category_url,category_name,category_id from nd_jhsitemgroup_parser_category'
            return self.jhs_db.select(sql)
        except Exception, e:
            print '# select Jhs itemgroup category end exception:', e

    # 查找已经结束的商品
    def selectJhsGroupItemEnd(self, args):
        try:
            sql = 'select item_juid,item_id from nd_jhsitemgroup_parser_item_info where end_time < %s'
            return self.jhs_db.select(sql, args)
        except Exception, e:
            print '# select Jhs itemgroup item end exception:', e

    # 查找没有结束的商品
    def selectJhsGroupItemNotEnd(self, args):
        try:
            sql = 'select b.category_url,a.item_juid,a.item_id,a.item_ju_url from nd_jhsitemgroup_parser_item_info a left join nd_jhsitemgroup_parser_category b on a.category_id = b.category_id where a.end_time >= %s'
            return self.jhs_db.select(sql, args)
        except Exception, e:
            print '# select Jhs itemgroup not end item exception:', e

    # 查找已经开团但是没有结束的商品
    def selectJhsGroupItemAlive(self, args):
        try:
            sql = 'select b.category_url,a.item_juid,a.item_id,a.item_ju_url from nd_jhsitemgroup_parser_item_info a left join nd_jhsitemgroup_parser_category b on a.category_id = b.category_id where a.start_time <= %s and a.end_time >= %s'
            return self.jhs_db.select(sql, args)
        except Exception, e:
            print '# select Jhs itemgroup alive item exception:', e

    # 新加商品信息
    def insertJhsGroupItemInfo(self, args_list):
        try:
            sql = 'call sp_jhsitemgroup_parser_item_info(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            self.jhs_db.executemany(sql, args_list)
        except Exception, e:
            print '# itemgroup insert Jhs Item info exception:', e

    # 每小时抓取商品
    def insertJhsGroupItemForHour(self, args_list):
        try:
            sql = 'call sp_jhsitemgroup_parser_item_h(%s,%s,%s,%s,%s)'
            self.jhs_db.executemany(sql, args_list)
        except Exception, e:
            print '# itemgroup insert Jhs Item for hour exception:', e

    # 商品位置信息
    def insertJhsGroupItemPosition(self, args_list):
        try:
            sql = 'call sp_jhsitemgroup_parser_item_position(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            self.jhs_db.executemany(sql, args_list)
        except Exception, e:
            print '# itemgroup insert Jhs item position exception:', e

    # 商品预热信息
    def insertJhsGroupItemComing(self, args_list):
        try:
            sql = 'call sp_jhsitemgroup_parser_item_coming(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            self.jhs_db.executemany(sql, args_list)
        except Exception, e:
            print '# itemgroup insert Jhs item position exception:', e
    

if __name__ == '__main__':
    my = MysqlAccess()
