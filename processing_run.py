#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2018/03/15
# @Author  : kingsley kwong
# @Site    :
# @File    : processsing_run.py
# @Software: tmall spider
# @Function:

import threading
import Tkinter as tk
import tkFileDialog
from util.get_engine import GetDBEngine
import pandas as pd
from multiprocessing import Process, Manager
from util.data_to_vertica import (write_db_process, WriteMainDataPack, WriteEvalDataPack)
import ConfigParser
from spider.tmall_spider import crawl_tmall_data

PROCESS_NUM = 5  # 进程数
root = tk.Tk()
root.withdraw()
config = ConfigParser.ConfigParser()
config.read('tmall.cfg')
raw_file_save_path = config.get('Save_Path_Config', 'save_file_path_root')
export_file_path = config.get('Export_Path_Config', 'export_path_root')
engine = GetDBEngine(config)


def get_goods_id(good_sn):
    engine_vertica = engine.vertica_engine()
    sql = '''SELECT outer_goods_id FROM huimei.e3_goods_outer_sku 
                  WHERE kehu_id=3 AND lylx=1 AND goods_sn='%s'
                  AND to_timestamp(approve_time)>'2018-02-01' ''' %good_sn
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
    manager = Manager()
    mq = manager.list()
    cq = manager.list()
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

    _process_db_1.join()
    _process_db_2.join()



if __name__ == '__main__':
    print(u'爬虫运行开始')
    file_name = tkFileDialog.askopenfilename(filetypes=[("csvformat", "csv")])
    lines = [line for line in read_file(file_name, 261120)]
    lines = ''.join(lines)
    lines = lines.replace('\n', '').split(',')
    lines.remove('')
    goods_sn = set(lines)
    map(spider, goods_sn)
    print(u'爬虫运行结束')


