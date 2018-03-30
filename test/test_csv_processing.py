#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2018/03/19
# @Author  : kingsley kwong
# @Site    :
# @File    : test_csv_processing.py
# @Software: vip spider
# @Function:

from util.csv_processing import (ExtractDataM,SaveAsCSVM)
percentageColumns = [u'售卖比（销售额）', u'转化率', u'退货率', u'同因拒退占比', u'sku售罄率', u'三级分类售卖比', u'第一天售卖比', u'拒收率',
                         u'拒退率', u'移动端销售量占比', u'移动端销售额占比', u'移动端转化率', u'移动端UV占比', u'广州数量售卖比',
                                                                                  u'上海数量售卖比', u'成都数量售卖比', u'北京数量售卖比',
                         u'武汉数量售卖比', u'数量售卖比', u'商品日均CTR']
files = [u'D:\\share\\vip_data\\temp\\7001\\sale\\商品明细_【3月真丝】 丝情画意-20180302_普通特卖_全部人群_全国_合计_条形码.xlsx',
         u'D:\\share\\vip_data\\temp\\7001\\uv\\商品明细_【3月真丝】 丝情画意-20180302_普通特卖_全部人群_全国_合计_条形码.xlsx']
header = ['shop', u'档期唯一码(档期名称+日期)', u'档期名称', u'售卖时间',
                    u'销售额（含满减含拒退）', u'销售额（扣满减含拒退）',
                    u'销售量（含拒退）', u'UV', u'购买人数', u'售卖比（销售额）',
                    u'退货率']

sacsvm = SaveAsCSVM(files_paths=["D:\\share\\vip_data\\temp\\7001\\uv","D:\\share\\vip_data\\temp\\7001\\sale"],
                    tb_frame_json='e:\\vip_spider\\util\\table_frame.json', export_path="D:\\share\\vip_data")
head = {'shop': 'shop'}
head.update({u'档期唯一码(档期名称+日期)': '111111'})
head.update({u'售卖时间': '22222'})
head.update({u'档期名称': '333333'})
sacsvm.save_process(u'商品明细_【3月真丝】 丝情画意-20180302_普通特卖_全部人群_全国_合计_条形码.xlsx', **head)
# edm = ExtractDataM(files=files, percentage_column=percentageColumns, save_path='D:\\share\\vip_data')
# edm.drag_datas_from_header(header)