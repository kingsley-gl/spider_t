#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2018/03/28
# @Author  : kingsley kwong
# @Site    :
# @File    : data_to_vertica.py
# @Software: tmall spider
# @Function:

import os
from .exceptions import DataBaseExecuteError
import time
import datetime
import sys
from util.logger import log

reload(sys)
sys.setdefaultencoding('utf8')

logger = log.getLogger('database_process')

def _class_logger(*dargs, **dkwargs):
    '''日志装饰器'''

    def decorator(func):
        def inner(self, *args, **kwargs):
            if dkwargs['level'] == 'debug':
                log = self.info.debug
            elif dkwargs['level'] == 'info':
                log = self.info.info
            if kwargs.has_key('tb_name'):
                log('%s %s' % (dkwargs['msg'], kwargs['tb_name']))
            else:
                log('%s' % dkwargs['msg'])
            func(self, *args, **kwargs)
            if kwargs.has_key('tb_name'):
                log('%s %s done' % (dkwargs['msg'], kwargs['tb_name']))
            else:
                log('%s done' % dkwargs['msg'])

        return inner

    return decorator


class WriteDataBase(object):

    def __init__(self, engine):
        self.engine = engine
        self.good_iid = None
        self.primary_key = None

    def __get_db_connect(self):
        return self.engine.vertica_engine()

    def __getattr__(self, item):
        if item == 'connect':
            try:
                return self.__get_db_connect()
            except DataBaseExecuteError:
                return None
        super(object, self).__getattr__()

    def do_sql(self, data_pack):
        pass

    def work(self, data_pack):
        try:
            self.do_sql(data_pack)
        except Exception as e:
            logger.error(e)
        finally:
            self.connect.close()



class WriteMainDataPack(WriteDataBase):
    def do_sql(self, data_pack):
        self.good_iid = data_pack['c_good_iid']
        self.primary_key = data_pack['c_primary_key']
        main_data = {}
        price_data = {}
        prop_data = {}
        for key, value in data_pack.items():
            if 'c_main' in key:
                main_data[key] = value
            elif 'c_price' in key:
                price_data[key] = value
            elif 'c_prop' in key:
                prop_data[key] = value

        # self.connect.setencoding(unicode, encoding='utf-8')
        with self.connect.cursor() as crsr:
            self._main_data_to_db(main_data, crsr)
            self._price_data_to_db(price_data, crsr)
            self._prop_data_to_db(prop_data, crsr)


    def _main_data_to_db(self, main_data, crsr):
        columns = list()
        for key, item in main_data.items():
            if isinstance(item, float):
                column = '='.join([key.replace('c_main_', ''),str(item)])
            else:
                column = '='.join([key.replace('c_main_', ''), "'"+item+"'"])
            columns.append(column)
        column_sql = ','.join(columns)
        # platform, cid, cat_name, outer_id, tsc, product_id,
        sql = '''UPDATE huimei.dc_platform_products_main SET %s                    
                  WHERE id = %s; ''' % (column_sql, self.primary_key)
        crsr.execute(sql)
        logger.info('main table insert success')

    def _prop_data_to_db(self, prop_data, crsr):
        columns = list()
        for key, value in prop_data.items():  # 需要优化效率
            for k, v in value.items():
                sql_insert = '''INSERT INTO huimei.dc_platform_products_prop_name (prop_name,prop_type,main_id,iid)
                                SELECT '%s','%s',%s,%s FROM dual
                                WHERE NOT EXISTS
                                (SELECT prop_name FROM huimei.dc_platform_products_prop_name 
                                WHERE prop_name='%s');''' % (k, key.replace('c_prop_', ''),
                                                             self.primary_key, self.good_iid, k)
                sql_select = ''' SELECT id FROM huimei.dc_platform_products_prop_name WHERE prop_name='%s'; ''' % k
                crsr.execute(sql_insert)
                crsr.execute(sql_select)
                tag_id = crsr.fetchall()[0][0]
                if len(v) > 4096:
                    v = v[0:4095]
                value_insert = '''INSERT INTO huimei.dc_platform_products_prop_value (main_id, iid, prop_name_id, prop_value)
                                    VALUES (%s, %s, %s, '%s')''' %(self.primary_key, self.good_iid, tag_id, v)
                crsr.execute(value_insert)

    def _price_data_to_db(self, price_data, crsr):
        columns = list()
        for value in price_data.values():
            for k, v in value.items():
                sql = '''INSERT INTO huimei.dc_platform_products_price (main_id, iid, price_name, price_value)
                          VALUES (%s, %s, %s, %s);''' % (self.primary_key, self.good_iid, "'"+k+"'", v)
                crsr.execute(sql)


class WriteEvalDataPack(WriteDataBase):
    def sql_format(self, column_list, value_list, tr_id, equals):
        with self.connect.cursor() as crsr:
            column = ','.join(column_list)
            value = ','.join(value_list)
            sql_sel = '''SELECT id FROM huimei.dc_platform_products_evaluation WHERE rate_tr_id=%s''' % tr_id
            crsr.execute(sql_sel)
            cmt_id = crsr.fetchall()
            if cmt_id:
                sql = '''UPDATE huimei.dc_platform_products_evaluation SET %s WHERE id=%s''' % (equals, str(cmt_id[0][0]))
            else:
                sql = '''INSERT INTO huimei.dc_platform_products_evaluation (%s) VALUES (%s)''' % (column, value)
            # logger.info(sql)
            crsr.execute(sql)

    def do_sql(self, data_pack):
        self.good_iid = data_pack['c_good_iid']
        self.primary_key = data_pack['c_primary_key']
        page_id = data_pack['c_page_id']

        for commt_floor in data_pack['c_eval_page_pack']:
            rate_tr_id = ''.join([page_id, '%02d' % commt_floor['floor']])
            p_equals = []
            p_columns = []
            p_values = []
            a_equals = []
            a_columns = []
            a_values = []
            equals = [''.join(['main_id=', self.primary_key]), ''.join(['iid=', self.good_iid])]
            columns = ['main_id', 'iid']
            values = [self.primary_key, self.good_iid]
            for key in commt_floor.keys():
                if 'commt_premiere_' in key:    # 初评
                    if commt_floor[key]:    # 防空值
                        if 'rate_date' in key:
                            p_column = key.replace('commt_premiere_', '')  # column
                            p_columns.append(p_column)
                            date = commt_floor[key]
                            if len(commt_floor[key]) < 10:
                                date = '.'.join(['2018', commt_floor[key]])
                            p_values.append("'"+date.replace('.', '-')+"'")  # date value
                            p_equal = '='.join(
                                [key.replace('commt_premiere_', ''), "'"+date.replace('.', '-')+"'"])  # column=value
                            p_equals.append(p_equal)
                        elif 'commt_premiere_rate_content' in key:
                            p_equal = '='.join(
                                [key.replace('commt_premiere_', ''), "'"+commt_floor[key]+"'"])  # column=value
                            p_equals.append(p_equal)
                            p_column = key.replace('commt_premiere_', '')
                            p_columns.append(p_column)
                            p_values.append("'"+commt_floor[key]+"'")
                            # logger.info("tr_id: %s 初次:%s" %(rate_tr_id,"'"+commt_floor[key]+"'"))
                            p_columns.append('rate_type')
                            p_values.append('1')
                        else:
                            p_equal = '='.join(
                                [key.replace('commt_premiere_', ''), "'"+commt_floor[key]+"'"])  # column=value
                            p_equals.append(p_equal)
                            p_column = key.replace('commt_premiere_', '')
                            p_columns.append(p_column)
                            p_values.append("'"+commt_floor[key]+"'")
                            # logger.info("tr_id: %s 初次解释:%s" % (rate_tr_id, "'" + commt_floor[key] + "'"))
                elif 'commt_append_' in key:     # 追评
                    if commt_floor[key] is not None:    # 防空值
                        if 'commt_append_rate_content' in key:
                            a_equal = '='.join(
                                [key.replace('commt_append_', ''), "'"+commt_floor[key]+"'"])  # column=value
                            a_equals.append(a_equal)
                            a_column = key.replace('commt_append_', '')
                            a_columns.append(a_column)
                            a_values.append("'"+commt_floor[key]+"'")
                            # logger.info("tr_id: %s 追加评论:%s" % (rate_tr_id, "'" + commt_floor[key] + "'"))
                            a_columns.append('rate_type')
                            a_values.append('2')
                        else:
                            a_equal = '='.join(
                                [key.replace('commt_append_', ''), "'"+commt_floor[key]+"'"])  # column=value
                            a_equals.append(a_equal)
                            a_column = key.replace('commt_append_', '')
                            a_columns.append(a_column)
                            a_values.append("'"+commt_floor[key]+"'")
                            # logger.info("tr_id: %s 追加解释:%s" % (rate_tr_id, "'" + commt_floor[key] + "'"))
                else:
                    if 'floor' in key:
                        column = 'rate_tr_id'
                        value = rate_tr_id
                        # columns.append(column)
                        # values.append(value)
                    else:
                        equal = '='.join(
                            [key.replace('commt_', ''), "'"+commt_floor[key]+"'"])  # column=value
                        equals.append(equal)
                        column = key.replace('commt_', '')
                        columns.append(column)
                        values.append("'"+commt_floor[key]+"'")

            pe = p_equals +equals
            pc = p_columns + columns
            pv = p_values + values
            pc.append('rate_tr_id')
            pv.append(rate_tr_id+'1')   # rate_tr_id : 6位page + iid + 2位floor + 评论type

            self.sql_format(pc, pv, rate_tr_id+'1', ','.join(pe))
            if a_columns:
                ae = a_equals + equals
                ac = a_columns + columns
                av = a_values + values
                ac.append('rate_tr_id')
                av.append(rate_tr_id+'2')
                self.sql_format(ac, av, rate_tr_id+'2', ','.join(ae))




def write_db_process(q, work_cls, engine, task_num):
    wcls = work_cls(engine)
    logger.info('pipe-%s: write db process start' % wcls)
    cnt = 0
    while True:
        try:
            data = q.pop(0)
            logger.info('pipe-%s: %s' % (wcls, len(q)))
            if isinstance(data, dict):
                wcls.work(data)
            elif isinstance(data, str):
                if 'close' in data:
                    cnt += 1
                if cnt >= task_num and len(q) == 0:
                    logger.info('pipe-%s: write db process stop' % wcls)
                    break
        except IndexError:
            pass

    logger.info('write db process stop')
