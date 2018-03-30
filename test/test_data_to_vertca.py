#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2018/03/19
# @Author  : kingsley kwong
# @Site    :
# @File    : test_file_to_vertica.py
# @Software: vip spider
# @Function:

from util.data_to_vertica import WriteMainDataPack
from util.get_engine import GetDBEngine
import ConfigParser

config = ConfigParser.ConfigParser()
config.read('../vip.cfg')
engine = GetDBEngine(config)
ftb = WriteMainDataPack(engine)
print('dt', ftb.connect)
print(ftb.connect)
print('dt', ftb.connect)
print('dt', ftb.connect)
