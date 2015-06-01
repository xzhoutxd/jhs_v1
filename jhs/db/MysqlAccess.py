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
    def insertJhsAct(self, args_list):
        try:
            sql = 'replace into nd_jhs_parser_activity(crawl_time,act_id,category_id,category_name,act_position,act_platform,act_channel,act_name,act_url,act_desc,act_logopic_url,act_enterpic_url,act_status,act_sign,_act_ids,seller_id,seller_name,shop_id,shop_name,discount,act_soldcount,act_remindnum,act_coupon,act_coupons,brand_id,brand_name,juhome,juhome_position,start_time,end_time,c_begindate,c_beginhour) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            #self.jhs_db.execute(sql, args)
            self.jhs_db.executemany(sql, args_list)
        except Exception, e:
            print '# insert Jhs Act exception:', e

    # 更新活动信息
    def updateJhsAct(self, args_list):
        try:
            sql = 'call sp_update_jhs_parser_activity(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            self.jhs_db.executemany(sql, args_list)
        except Exception, e:
            print '# update Jhs Act exception:', e

    """
    # 新加商品
    def insertJhsItem(self, args_list):
        try:
            sql = 'replace into nd_jhs_parser_item(crawl_time,item_juid,act_id,act_name,act_url,item_position,item_ju_url,item_juname,item_judesc,item_jupic_url,item_id,item_url,seller_id,seller_name,shop_id,shop_name,shop_type,item_oriprice,item_actprice,discount,item_remindnum,item_soldcount,item_stock,item_prepare,item_favorites,item_promotions,cat_id,brand_name) value(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            #self.jhs_db.execute(sql, args)
            self.jhs_db.executemany(sql, args_list)
        except Exception, e:
            print '# insert Jhs Item exception:', e
    """

    # 新加商品信息
    def insertJhsItemInfo(self, args_list):
        try:
            #sql = 'replace into nd_jhs_parser_item_info(crawl_time,item_juid,act_id,act_name,act_url,item_position,item_ju_url,item_juname,item_judesc,item_jupic_url,item_id,item_url,seller_id,seller_name,shop_type,item_oriprice,item_actprice,discount,item_remindnum,item_promotions,act_starttime) value(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            #self.jhs_db.execute(sql, args)
            sql = 'call sp_jhs_parser_item_mid_info(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            self.jhs_db.executemany(sql, args_list)
        except Exception, e:
            print '# insert Jhs Item info exception:', e

    # 更新商品信息
    def updateJhsItem(self, args):
        try:
            sql = 'call sp_update_item(%s,%s,%s,%s,%s)'
            self.jhs_db.execute(sql, args)
        except Exception, e:
            print '# update Jhs Item exception:', e

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
    def insertJhsActComing(self, args_list):
        try:
            #sql  = 'replace into nd_jhs_parser_activity_coming(crawl_time,act_id,category_id,category_name,act_position,act_platform,act_channel,act_name,act_url,act_desc,act_logopic_url,act_enterpic_url,act_status,act_sign,_act_ids,seller_id,seller_name,shop_id,shop_name,discount,act_soldcount,act_remindnum,act_coupon,act_coupons,brand_id,brand_name,juhome,juhome_position,start_time,end_time,c_begindate,c_beginhour) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            sql = 'call sp_jhs_parser_activity_coming(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            self.jhs_db.executemany(sql, args_list)
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
    def selectJhsActNotStart(self, args=None):
        try:
            if args:
                sql = 'select * from nd_jhs_parser_activity where start_time > %s and act_sign != 3'
                return self.jhs_db.select(sql, args)
            else:
                sql = 'select * from nd_jhs_parser_activity where start_time > now()'
                return self.jhs_db.select(sql)
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

    # 查找还有一小时开团的活动
    def selectJhsActStartForOneHour(self, args):
        # 非俪人购
        try:
            sql = 'select * from nd_jhs_parser_activity where start_time > %s and start_time < %s and act_sign != 3'
            return self.jhs_db.select(sql, args)
        except Exception, e:
            print '# select Jhs alive act exception:', e

    # 查找需要抓取销量、关注人数的商品 按照活动Id查找
    def selectJhsItemsFromActId(self, args):
        try:
            #sql = 'select a.item_juid,a.act_id,a.item_ju_url,b.act_url,a.item_id from nd_jhs_parser_item_info a join nd_jhs_parser_activity b on a.act_id = b.act_id where a.act_id = %s'
            sql = 'select b.item_juid,a.act_id,b.item_ju_url,b.act_url,b.item_id from nd_jhs_parser_activity_item a join nd_jhs_parser_item_info b on a.item_juid = b.item_juid where a.act_id = %s'
            return self.jhs_db.select(sql, args)
        except Exception, e:
            print '# select Jhs act items for sale and remind exception:', e

    # 查找需要更新的商品信息
    def selectJhsItemsForUpdate(self, args):
        try:
            sql = 'select b.act_id,b.act_name,a.item_juid,b.act_id,a.item_ju_url,b.act_url,a.item_id from nd_jhs_parser_item_info a join nd_jhs_parser_activity b on a.act_id = b.act_id where a.start_time > %s and a.start_time < %s'
            return self.jhs_db.select(sql, args)
        except Exception, e:
            print '# select Jhs update items exception:', e

    # 需要每天抓取的活动
    def insertJhsActDayalive(self, args_list):
        try:
            sql = 'insert ignore into nd_jhs_parser_activity_alive_d(act_id,category_id,category_name,act_name,act_url,_start_time,_end_time,c_begindate,c_beginhour) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            #self.jhs_db.execute(sql, args)
            self.jhs_db.executemany(sql, args_list)
        except Exception, e:
            print '# insert Jhs alive act for day exception:', e

    # 查找需要每天抓取的活动
    def selectJhsActDayalive(self, args):
        # 非俪人购
        # 当前时间减一天小于结束时间，需要每天抓取
        try:
            sql = 'select * from nd_jhs_parser_activity_alive_d where _start_time < %s and _end_time >= %s'
            return self.jhs_db.select(sql, args)
        except Exception, e:
            print '# select Jhs alive act for day exception:', e

    # 从每天抓取表中查找需要删除已经结束的活动
    def selectDeleteJhsActDayalive(self, args):
        # 当前时间减一天大于结束时间，需要删除
        try:
            sql = 'select * from nd_jhs_parser_activity_alive_d where _end_time < %s'
            return self.jhs_db.select(sql, args)
        except Exception, e:
            print '# select need delete Jhs alive act for day exception:', e

    # 从每天抓取表中删除已经结束的活动
    def deleteJhsActDayalive(self, args):
        # 当前时间减一天大于结束时间，需要删除
        try:
            sql = 'delete from nd_jhs_parser_activity_alive_d where _end_time < %s'
            self.jhs_db.execute(sql, args)
        except Exception, e:
            print '# delete Jhs alive act for day exception:', e

    # 查找需要每天抓取活动的商品 按照活动Id查找
    def selectJhsItemsDayalive(self, args):
        try:
            sql = 'select a.item_juid,a.act_id,a.act_name,a.act_url,a.item_juname,a.item_ju_url,a.item_id,a.item_url,a.item_oriprice,a.item_actprice from nd_jhs_parser_item_info as a join nd_jhs_parser_activity_alive_d as b on a.act_id = b.act_id where b.act_id = %s'
            return self.jhs_db.select(sql, args)
        except Exception, e:
            print '# select Jhs alive act items for day exception:', e

    # 需要小时抓取的活动
    def insertJhsActHouralive(self, args_list):
        try:
            sql = 'insert ignore into nd_jhs_parser_activity_alive_h(act_id,category_id,category_name,act_name,act_url,_start_time,_end_time,c_begindate,c_beginhour) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            #self.jhs_db.execute(sql, args)
            self.jhs_db.executemany(sql, args_list)
        except Exception, e:
            print '# insert Jhs alive act for hour exception:', e

    # 查找需要小时抓取的活动
    def selectJhsActHouralive(self, args):
        # 非俪人购
        # (当前时间减去最小时间段大于开始时间, 当前时间减去最大时间段小于开始时间) 需要每小时抓取
        try:
            sql = 'select * from nd_jhs_parser_activity_alive_h where _start_time <= %s and _start_time >= %s'
            return self.jhs_db.select(sql, args)
        except Exception, e:
            print '# select Jhs alive act for hour exception:', e

    # 从每小时抓取表中查找需要删除已经超过统计时段的活动
    def selectDeleteJhsActHouralive(self, args):
        # 当前时间减去最大时间段大于开始时间，需要删除
        try:
            sql = 'select * from nd_jhs_parser_activity_alive_h where _start_time < %s'
            return self.jhs_db.select(sql, args)
        except Exception, e:
            print '# select need delete Jhs alive act for hour exception:', e

    # 从每小时抓取表中删除已经超过统计时段的活动
    def deleteJhsActHouralive(self, args):
        # 当前时间减去最大时间段大于开始时间，需要删除
        try:
            sql = 'delete from nd_jhs_parser_activity_alive_h where _start_time < %s'
            self.jhs_db.execute(sql, args)
        except Exception, e:
            print '# delete Jhs alive act for hour exception:', e

    # 查找需要每小时抓取活动的商品 按照活动Id查找
    def selectJhsItemsHouralive(self, args):
        try:
            #sql = 'select a.item_juid,a.act_id,a.act_name,a.act_url,a.item_juname,a.item_ju_url,a.item_id,a.item_url,a.item_oriprice,a.item_actprice from nd_jhs_parser_item as a join nd_jhs_parser_activity_alive_h as b on a.act_id = b.act_id where b.act_id = %s'
            sql = 'select a.item_juid,a.act_id,a.item_ju_url,a.act_url,a.item_id from nd_jhs_parser_item_info as a join nd_jhs_parser_activity_alive_h as b on a.act_id = b.act_id where b.act_id = %s'
            #sql = 'select c.item_juid,a.act_id,c.item_ju_url,c.act_url,c.item_id from nd_jhs_parser_activity_alive_h as a join nd_jhs_parser_activity_item b on a.act_id = b.act_id join nd_jhs_parser_item_info as c on b.item_juid = c.item_juid where a.act_id = %s'
            return self.jhs_db.select(sql, args)
        except Exception, e:
            print '# select Jhs alive act items for hour exception:', e

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
