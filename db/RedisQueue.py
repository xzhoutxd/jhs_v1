#-*- coding:utf-8 -*-
#!/usr/bin/env python

from sys import path

from hotqueue import HotQueue
path.append(r'../base')
import Environ as Environ

class RedisQueue:
    def __init__(self):
        # redis数据库设置
        self.QUEUE_DB = 100

        # redis配置
        self.redis_ip, self.redis_port, self.redis_passwd = Environ.redis_config[self.QUEUE_DB]

        # 抓取队列   tm:1, tb:2, vip:3, jhs:4
        self.q_list = [
                '4_cat_home', '4_cat_homeposition', '4_cat_main', '4_cat_position', '4_act_main', '4_act_check', '4_act_position', '4_item_update', '4_item_hour', '4_item_day', '4_item_check', # 聚划算品牌团 类别 活动 商品
                '4_groupitem_hour', '4_groupitemcat_main' # 聚划算商品团 商品每小时销售, 类别每小时商品
            ]

        # 初始化队列
        self.initQueue()

    def initQueue(self):
        # hotqueue队列字典表
        self.q_dict = {}

        # 抓取队列
        for q in self.q_list:
            self.q_dict[q] = HotQueue(q, host=self.redis_ip, port=self.redis_port, password=self.redis_passwd, db=self.QUEUE_DB)

    # To put queue
    def put_q(self, _key, _val):
        try:
            if self.q_dict.has_key(_key):
                q = self.q_dict[_key]
                q.put(_val)
        except Exception as e:
            print '# put_q exception ', e

    # To get queue
    def get_q(self, _key):
        _val = None
        try:
            if self.q_dict.has_key(_key):
                q = self.q_dict[_key]
                _val = q.get()
        except Exception as e:
            print '# get_q exception ', e        
        return _val

    # 清空队列
    def clear_q(self, _key):
        if self.q_dict.has_key(_key):
            q = self.q_dict[_key]
            q.clear()

if __name__ == "__main__":
    q = RedisQueue()
#     q.clear_q('3_act')
#     q.clear_q('3_sku_h')
#     q.clear_q('3_sku_d')

