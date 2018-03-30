#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2018/03/19
# @Author  : kingsley kwong
# @Site    :
# @File    : test_get_engine.py
# @Software: vip spider
# @Function:

import ConfigParser
from util.get_engine import GetDBEngine

config = ConfigParser.ConfigParser()
config.read('../vip.cfg')
engine=GetDBEngine(config)
engine_vertica=engine.vertica_engine()
vertica_cur=engine_vertica.cursor()
sql = r'select * from sycm_user where user_id>=7000 and user_id<8000 and status=0'
vertica_cur.execute(sql)
engine_vertica.commit()
print(vertica_cur.fetchone())