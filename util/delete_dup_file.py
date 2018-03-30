#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2018/03/15
# @Author  : kingsley kwong
# @Site    :
# @File    : vip_spider_download_salesfile.py
# @Software: vip spider
# @Function:

import os

def delete_duplicate_files(rawFileSavePath):
    '''
    删除重复文件，大小为0的文件
    :param rawFileSavePath:
    :return:
    '''
    # for file in os.listdir(rawFileSavePath):
    #     try:
    #         fileName = file.decode('gbk')
    #         if '商品详情' in fileName and '货号' not in fileName and '条形码' not in fileName:
    #             titles = xlrd.open_workbook(filename=rawFileSavePath + '\%s' % fileName).sheet_by_index(0).row_values(0)
    #             if '条形码' in titles:
    #                 try:
    #                     parenthasisBeginPosition = fileName.find('(')
    #                 except:
    #                     parenthasisBeginPosition = 0
    #                 if parenthasisBeginPosition > 0:
    #                     os.rename(rawFileSavePath + '\%s' % fileName, rawFileSavePath + '\%s' % fileName[:parenthasisBeginPosition] + '_条形码.xlsx')
    #             else:
    #                 os.remove(rawFileSavePath + '\%s' % fileName)
    #     except:pass


    for file in os.listdir(rawFileSavePath):    # 删除重复数据
        try:
            fileName = file.decode('gbk')
            if '(' in fileName and ')' in fileName:
                os.remove(rawFileSavePath + '\%s' % fileName)
        except:pass


    for file in os.listdir(rawFileSavePath): # 删除大小为0的文档
        fileName = file.decode('gbk')
        if os.path.getsize(rawFileSavePath + '\%s' % fileName) == 0:
            os.remove(rawFileSavePath + '\%s' % fileName)