#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2018/03/20
# @Author  : kingsley kwong
# @Site    :
# @File    : test_crawler.py
# @Software: vip spider
# @Function:

from spider.vip_spider_download_salesfile import crawl_salesfile_data
from spider.vip_spider_download_uvfile import crawl_uvfile_data
a = []
crawl_uvfile_data(login_user='jiangshan@inman.cc', password='Abcd1234#', download_path='aaa',
                     crawl_days=1, crawl_dates=['2018-03-08'], share_list=a)
# print(a)