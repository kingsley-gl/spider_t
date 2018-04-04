#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2018/03/15
# @Author  : kingsley kwong
# @Site    :
# @File    : processsing_run.py
# @Software: tmall spider
# @Function:

import os
import ConfigParser
from Queue import Queue
from threading import Thread
import Tkinter as tk
import tkFileDialog
from util.get_engine import GetDBEngine
import pandas as pd

config = ConfigParser.ConfigParser()
config.read('tmall.cfg')
PROCESS_NUM = int(config.get('PROCESS_NUM', 'process'))  # 进程数
THREAD_NUM = int(config.get('THREAD_NUM', 'thread'))  # 线程数
path1 = config.get('SPIDER_LOG', 'log_file_path')
path2 = config.get('DATABASE_LOG', 'log_file_path')
path1 = os.path.abspath(os.path.dirname(path1))
path2 = os.path.abspath(os.path.dirname(path2))
if not os.path.exists(path1):
    os.makedirs(path1)
if not os.path.exists(path2):
    os.makedirs(path2)

from multiprocessing import Process, Pool
from util.data_to_vertica import (write_db_process, WriteMainDataPack, WriteEvalDataPack)
from spider.tmall_spider import crawl_tmall_data

root = tk.Tk()
root.withdraw()
engine = GetDBEngine(config)


def get_goods_id(id_sn):
    engine_vertica = engine.vertica_engine()
    sql = '''SELECT outer_goods_id FROM huimei.e3_goods_outer_sku 
                  WHERE kehu_id=%s AND lylx=1 AND goods_sn='%s'
                  AND to_timestamp(approve_time)>'2018-02-01' ''' % (id_sn[1], id_sn[0])
    outer_goods_id = pd.read_sql(sql, engine_vertica)
    outer_goods_id = set(list(outer_goods_id['outer_goods_id']))
    return outer_goods_id


def read_file(name, chunk_size):
    try:
        file_obj = open(name)
        file_obj.seek(0)
        while True:
            pos = file_obj.tell()
            line = file_obj.read(chunk_size)
            if line:
                yield line
            if pos == file_obj.tell():
                break
    finally:
        file_obj.close()


def spider(good_sn):
    _process_alive = []  # 正在运行进程
    _process_dead = []  # 已经运行进程，死进程
    set_of_goods_id = get_goods_id(good_sn)
    if not set_of_goods_id:
        print(u'%s 该款号在数据源中不存在！' % (good_sn[0]))
        return None
    mq = Queue()
    cq = Queue()
    _processes_pool = [Thread(target=crawl_tmall_data,
                               kwargs={'good_iid': good_id,
                                       'main_data_queue': mq,
                                       'comment_data_queue': cq,
                                       'engine': engine}) for good_id in set_of_goods_id]
    task_num = len(_processes_pool)
    _process_db_1 = Thread(target=write_db_process, args=[mq, WriteMainDataPack, engine, task_num])  # 写入主表进程
    _process_db_2 = Thread(target=write_db_process, args=[cq, WriteEvalDataPack, engine, task_num])  # 写入评论表进程
    _process_db_1.start()
    _process_db_2.start()
    while _processes_pool or _process_alive:
        if len(_process_alive) < THREAD_NUM and _processes_pool:
            p = _processes_pool.pop()
            p.start()
            _process_alive.append(p)

        if _process_alive:
            for pa in _process_alive:
                if not pa.is_alive():
                    _process_alive.remove(pa)
                    _process_dead.append(pa)

    _process_db_1.join()
    _process_db_2.join()


# if __name__ == '__main__':
#     print(u'爬虫运行开始')
#     p = Pool(PROCESS_NUM)
#     file_name = tkFileDialog.askopenfilename(filetypes=[("xlsx format", "xlsx"), ("xls format", "xls")])
#     data = pd.read_excel(file_name,  na_values=['NA'])
#     data = data.drop_duplicates()
#
#     # lines = [line for line in read_file(file_name, 261120)]
#     # lines = ''.join(lines)
#     # lines = lines.replace('\n', '').split(',')
#     # lines.remove('')
#     # goods_sn = set(lines)
#     p.map(spider, (row for i, row in data.iterrows()))
#     print(u'爬虫运行结束')


