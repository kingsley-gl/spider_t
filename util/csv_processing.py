#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2018/03/15
# @Author  : kingsley kwong
# @Site    :
# @File    : csv_processing.py
# @Software: vip spider
# @Function:

import pandas as pd
import datetime
import time
import os
import re
import json
from util.logger import log
logger = log.getLogger('spider_info')
class ExtractDataM(object):
    def __init__(self, files, percentage_column, save_path=None):
        '''

        :param files: files list
        :param percentage_column:
        :param save_path:
        '''
        self.save_file_path = save_path #csv to save
        self.files = files
        self.files_xls = map(pd.ExcelFile,self.files)
        self.column_dict = {}
        self.percentage_column = percentage_column #percentage column list
        self._result = None
        self.table = None   #table name

    def drag_datas_from_header(self, headers):
        '''
        提取数据
        :param headers:
        :param sale_sheet_name:
        :param uv_sheet_name:
        :return:
        '''
        dfs = map(pd.read_excel, self.files_xls)
        dfs = map(lambda x: x.dropna(axis=0), dfs)

        for p_header in self.percentage_column: # 百分比数据处理
            for df in dfs:
                if p_header in df.columns:
                    if len(df[p_header]) > 0:
                        df[p_header] = [str(element).replace('%', '') if '%' in str(element)
                                             else str(element) for element in list(df[p_header])]
                        df[p_header] = [float(element)/100 if re.match('\d+', str(element)) is not None
                                             else 0.0 for element in list(df[p_header])]
        df_cols = []
        for df in dfs:
            df_col = [u'条形码']
            for header in headers:
                if header in df.columns:
                    df_col.append(header)   # add column from header
            df_col = set(df_col)
            df_cols.append(df_col)

        target_dfs = []
        for df, cols in zip(dfs, df_cols):
            temp = df.loc[:, cols]  # getting header data
            temp = temp.sort_values(by=[u'条形码'])
            target_dfs.append(temp)

        for i in range(len(target_dfs) - 1):
            if not i:
                result_df = pd.merge(target_dfs[i], target_dfs[i+1], on=u'条形码')  # merge different dataframe
            else:
                result_df = pd.merge(result_df, target_dfs[i+1], on=u'条形码')

        for key in self.column_dict.keys():
            result_df[key] = self.column_dict[key]  # adding columns
        result_df = result_df.loc[:, headers]   # generate target dateframe
        return result_df

    def write_to_csv(self,result):
        '''
        导入csv
        :param result: drag_datas_from_header 得到的pandas表

        :return:
        '''
        path = self.save_file_path + r'\%s' % self.table + r'\%s_%s.%d' % (
        self.table, datetime.date.today().strftime('%Y-%m-%d'), time.time()) + '.csv'
        logger.info('start to writer table "%s" to file %s' % (self.table, path))
        if not os.path.exists(self.save_file_path + ur'\%s' % self.table):
            os.makedirs(self.save_file_path + ur'\%s' % self.table)
        result.to_csv(path_or_buf=path, header=False, index=False, encoding='utf-8')

    def add_column(self, key, value):
        '''
        添加列
        :param key:
        :param value:
        :return:
        '''
        self.column_dict[key] = value


class SaveAsCSVM(object):

    def __init__(self, files_paths, tb_frame_json, export_path):
        self.files_paths = files_paths
        with open(tb_frame_json) as fp:
            j = json.load(fp)
            self.tables = j['tables']
            self.percentage_columns = j['percentageColumns']
        self.export_path = export_path

    def save_process(self, file_name, **kwargs):
        files_list = map(lambda x: '\\'.join((x, file_name)), self.files_paths)

        e = ExtractDataM(files=files_list,
                         save_path=self.export_path,
                         percentage_column=self.percentage_columns)
        for key in kwargs.keys():
            e.add_column(key, kwargs[key])
        for table in self.tables:
            for key in table.keys():
                e.table = key
                ret = e.drag_datas_from_header(headers=table[key])
                e.write_to_csv(ret)


# class ExtractData(object):
#     def __init__(self, sales_file_name, uv_file_name, percentage_column, save_path=None):
#         self.save_file_path = save_path #csv to save
#         self.sales_file_name = sales_file_name  #sale excel file
#         self.uv_file_name = uv_file_name    #uv excel file
#         self.sales_xls = pd.ExcelFile(self.sales_file_name)
#         self.uv_xls = pd.ExcelFile(self.uv_file_name)
#         self.column_dict = {}
#         self.percentage_column = percentage_column #percentage column list
#         self._result = None
#         self.table = None   #table name
#
#     def drag_datas_from_header(self, headers, sale_sheet_name='sheet', uv_sheet_name='sheet'):
#         '''
#         提取数据
#         :param headers:
#         :param sale_sheet_name:
#         :param uv_sheet_name:
#         :return:
#         '''
#         sale_df = pd.read_excel(self.sales_xls, sheet_name=sale_sheet_name, )
#         # iterator = True ,chunksize = 10000)
#         uv_df = pd.read_excel(self.uv_xls, sheet_name=uv_sheet_name, )
#         # iterator = True ,chunksize = 10000)
#         sale_df = sale_df.dropna(axis=0)
#         uv_df = uv_df.dropna(axis=0)
#
#         for p_header in self.percentage_column: # 百分比数据处理
#             if p_header in sale_df.columns:
#                 if len(sale_df[p_header]) > 0:
#                     sale_df[p_header] = [str(element).replace('%', '') if '%' in str(element)
#                                          else str(element) for element in list(sale_df[p_header])]
#                     sale_df[p_header] = [float(element)/100 if re.match('\d+', str(element)) is not None
#                                          else 0.0 for element in list(sale_df[p_header])]
#             if p_header in uv_df.columns:
#                 if len(uv_df[p_header]) > 0:
#                     uv_df[p_header] = [str(element).replace('%', '') if '%' in str(element)
#                                        else str(element) for element in list(uv_df[p_header])]
#                     uv_df[p_header] = [float(element)/100 if re.match('\d+', str(element)) is not None
#                                        else 0.0 for element in list(uv_df[p_header])]
#
#         sale_col = [u'条形码']
#         uv_col = [u'条形码']
#
#         for header in headers:
#             if header in sale_df.columns:
#                 sale_col.append(header)
#             if header in uv_df.columns:
#                 uv_col.append(header)
#
#         sale_col = set(sale_col)
#         uv_col = set(uv_col)
#         sale_df = sale_df.loc[:, sale_col]  #table column data getting
#         uv_df = uv_df.loc[:, uv_col]
#         sale_df = sale_df.sort_values(by=[u'条形码'])  #sorting by barcode
#         uv_df = uv_df.sort_values(by=[u'条形码'])
#         result = pd.merge(sale_df, uv_df, on=u'条形码')    #merge tables
#         for key in self.column_dict.keys():
#             result[key] = self.column_dict[key]     #add column data
#         self._result = result.loc[:, headers]   #former table
#         return self._result
#
#     def write_to_csv(self,result):
#         '''
#         导入csv
#         :param result: drag_datas_from_header 得到的pandas表
#
#         :return:
#         '''
#
#         path = self.save_file_path + r'\%s' % self.table + r'\%s_%s.%d' % (
#         self.table, datetime.date.today().strftime('%Y-%m-%d'), time.time()) + '.csv'
#         if not os.path.exists(self.save_file_path + ur'\%s' % self.table):
#             os.makedirs(self.save_file_path + ur'\%s' % self.table)
#         result.to_csv(path_or_buf=path, header=False, index=False, encoding='utf-8')
#
#     def add_column(self, key, value):
#         '''
#         添加列
#         :param key:
#         :param value:
#         :return:
#         '''
#         self.column_dict[key] = value
#
#     # funture
#     # def __enter__(self):
#     #     pass
#     # def __exit__(self, exc_type, exc_val, exc_tb):
#     #     pass
#
#
# class SaveAsCSV(object):
#     percentageColumns = [u'售卖比（销售额）', u'转化率', u'退货率', u'同因拒退占比', u'sku售罄率', u'三级分类售卖比', u'第一天售卖比', u'拒收率',
#                          u'拒退率', u'移动端销售量占比', u'移动端销售额占比', u'移动端转化率', u'移动端UV占比', u'广州数量售卖比',
#                             u'上海数量售卖比', u'成都数量售卖比', u'北京数量售卖比',
#                          u'武汉数量售卖比', u'数量售卖比', u'商品日均CTR']
#
#     tables = [
#         {'vip_active': [u'档期唯一码(档期名称+日期)', u'档期名称', u'售卖时间', ]},
#
#         {'vip_active_day': ['shop', u'档期唯一码(档期名称+日期)', u'档期名称', u'售卖时间',
#                             u'销售额（含满减含拒退）', u'销售额（扣满减含拒退）',
#                             u'销售量（含拒退）', u'UV', u'购买人数', u'售卖比（销售额）',
#                             u'退货率']},
#
#         {'vip_active_hour': ['shop', u'档期唯一码(档期名称+日期)', u'售卖时间', u'时间',
#                              u'销售额（扣满减含拒退）', u'当日累计总销售额', u'档期累计总销售额',
#                              u'销售量（含拒退）', u'当日累计总销售量', u'档期累计总销售量',
#                              u'购买人数', u'当日累计购买人数', u'档期累计购买人数']},
#
#         {'vip_return': ['shop', u'档期唯一码(档期名称+日期)', u'商品详情链接', u'货号',
#                         u'条形码', u'商品名称', u'售卖价', u'销售量(含拒退)', u'退货量',
#                         u'拒收量', u'同因拒退占比', u'退货原因', u'地区']},
#
#         {'vip_goods': ['shop', u'档期唯一码(档期名称+日期)', u'商品详情页', u'商品名称',
#                        u'品牌名称', u'商品排位', u'货号', u'三级品类类型', u'正品价', u'售价 ',
#                        u'材质', u'售卖周期', u'总备货量', u'备货值（货值）',
#                        u'销售额（含满减含拒退）', u'销售量（含拒退）', u'库存量',
#                        u'销售额（含满减含拒退）', u'满减金额', u'UV', u'客单价', u'转化率',
#                        u'购买人数', u'售卖比（销售额）', u'售罄状态(字符类型)', u'sku售罄率',
#                        u'三级分类销售量', u'三级分类销售额', u'三级分类售卖比', u'第一天销售量',
#                        u'第一天售卖比', u'推荐等级', u'拒收量', u'拒收率', u'拒收金额', u'退货量',
#                        u'退货率', u'退货金额', u'拒退量', u'拒退率', u'拒退额', u'移动端销售量',
#                        u'移动端销售额', u'移动端销售量占比', u'移动端销售额占比', u'移动端UV',
#                        u'移动端购买人数', u'移动端转化率', u'移动端UV占比']},
#
#         {'vip_barCode': ['shop', u'档期唯一码(档期名称+日期)', u'商品详情页', u'商品名称',
#                          u'品牌名称', u'货号', u'条形码', u'尺码', u'款式细类', u'正品价',
#                          u'售价 ', u'材质', u'售卖周期', u'总备货量', u'备货值（货值）',
#                          u'总销售额（未扣满减未扣拒退）', u'总销售量（未扣拒退）', u'库存量',
#                          u'销售额(扣满减未扣拒退)', u'满减金额', u'uv', u'客单价', u'转化率',
#                          u'购买人数', u'售卖比', u'第一天销售量', u'第一天售卖比', u'拒收量',
#                          u'拒收率', u'拒收金额', u'退货量', u'退货率', u'退货金额', u'拒退量',
#                          u'拒退率', u'拒退额', u'退货原因', u'移动端销售量', u'移动端销售额',
#                          u'移动端销售量占比', u'移动端销售额占比', u'移动端UV', u'移动端购买人数',
#                          u'移动端转化率', u'移动端UV占比']},
#
#         {'vip_region': ['shop', u'档期唯一码(档期名称+日期)', u'品牌名称', u'商品名称',
#                         u'商品详情页', u'货号', u'SKU数', u'款式细类', u'广州备货量',
#                         u'上海备货量', u'成都备货量', u'北京备货量', u'武汉备货量', u'总备货量',
#                         u'广州销售量', u'上海销售量', u'成都销售量', u'北京销售量', u'武汉销售量',
#                         u'销售量（含拒退）', u'广州数量售卖比', u'上海数量售卖比',
#                         u'成都数量售卖比', u'北京数量售卖比', u'武汉数量售卖比', u'数量售卖比',
#                         u'广州购买人数', u'上海购买人数', u'成都购买人数', u'北京购买人数',
#                         u'武汉购买人数', u'购买人数']},
#
#         {'vip_behind_goods': ['shop', u'档期唯一码(档期名称+日期)', u'报表类型', u'商品名称',
#                               u'排列位置', u'货号', u'条形码', u'尺码', u'售卖价 ', u'进货量',
#                               u'销售量', u'售卖比', u'累计货值', u'销售额（扣满减含拒退）', u'UV', u'转化率',
#                               u'拒退量', u'退货率']}, ]
#
#     def __init__(self, uv_path, sale_path, export_path):
#         self.uv_path = uv_path
#         self.sale_path = sale_path
#         self.export_path = export_path
#
#     def save_process(self, file_name, **kwargs):
#         e = ExtractData(sales_file_name=self.sale_path+r'\%s'%file_name,
#                         uv_file_name=self.uv_path+r'\%s'%file_name,
#                         save_path=self.export_path,
#                         percentage_column=self.percentageColumns)
#         for key in kwargs.keys():
#             e.add_column(key,kwargs[key])
#         for table in self.tables:
#             for key in table.keys():
#                 e.table = key
#                 ret = e.drag_datas_from_header(headers=table[key])
#                 e.write_to_csv(ret)
#
