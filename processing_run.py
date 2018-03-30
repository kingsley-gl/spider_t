#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2018/03/15
# @Author  : kingsley kwong
# @Site    :
# @File    : vip_spider_download_uvfile.py
# @Software: vip spider
# @Function:

from util.get_engine import GetDBEngine
import pandas as pd
from multiprocessing import Process, Manager
from util.data_to_vertica import (write_db_process, WriteMainDataPack, WriteEvalDataPack)
import ConfigParser
from spider.tmall_spider import crawl_tmall_data

PROCESS_NUM = 5  # 进程数

config = ConfigParser.ConfigParser()
config.read('tmall.cfg')
raw_file_save_path = config.get('Save_Path_Config', 'save_file_path_root')
export_file_path = config.get('Export_Path_Config', 'export_path_root')
engine = GetDBEngine(config)
_process_alive = []  # 正在运行进程
_processes_pool = []  # 所有进程
_process_dead = []  # 已经运行进程，死进程

def get_goods_id(good_sn):
    engine_vertica = engine.vertica_engine()
    sql = '''SELECT outer_goods_id FROM huimei.e3_goods_outer_sku 
                  WHERE kehu_id=3 AND lylx=1 AND goods_sn='%s'
                  AND to_timestamp(approve_time)>'2018-02-01' ''' %good_sn
    outer_goods_id = pd.read_sql(sql, engine_vertica)
    outer_goods_id = set(list(outer_goods_id['outer_goods_id']))
    return outer_goods_id


if __name__ == '__main__':
    print(u'爬虫运行开始')
    manager = Manager()
    mq = manager.list()
    cq = manager.list()
    # print(cq, mq)
    good_sn = '8430820882'
    set_of_goods_id = get_goods_id(good_sn)
    _processes_pool = [Process(target=crawl_tmall_data,
                               kwargs={'good_iid': good_id,
                                       'main_data_queue': mq,
                                       'comment_data_queue': cq,
                                       'engine': engine}) for good_id in set_of_goods_id]
    task_num = len(_processes_pool)
    _process_db_1 = Process(target=write_db_process, args=[mq, WriteMainDataPack, engine, task_num])  # 写入主表进程
    _process_db_2 = Process(target=write_db_process, args=[cq, WriteEvalDataPack, engine, task_num])  # 写入评论表进程
    _process_db_1.start()
    _process_db_2.start()
    while _processes_pool or _process_alive:
        if len(_process_alive) < PROCESS_NUM and _processes_pool:
            p = _processes_pool.pop()
            p.start()
            _process_alive.append(p)

        if _process_alive:
            for pa in _process_alive:
                if not pa.is_alive():
                    _process_alive.remove(pa)
                    _process_dead.append(pa)


    # print(u'爬虫运行结束')
    _process_db_1.join()
    _process_db_2.join()

