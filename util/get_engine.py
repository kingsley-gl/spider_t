#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2018/03/19
# @Author  : kingsley kwong
# @Site    :
# @File    : get_engine.py
# @Software: vip spider
# @Function:

from .exceptions import DataBaseConnectError
import re
# from sqlalchemy import create_engine


class GetDBEngine(object):
    # pool = [] #链接池
    def __init__(self, config):
        for section in filter(lambda sec: u'DB' in sec,config.sections()):
            for option in config.options(section):
                self.__dict__[section[0:5]+'_'+option] = config.get(section,option)

    def vertica_engine(self):
        try:
            import pyodbc
            return pyodbc.connect('Driver={Vertica}; Database=%s; Servername=%s; UID=%s; PWD=%s; Port =%s'
                                  % (self.VERTI_db, self.VERTI_host, self.VERTI_user, self.VERTI_passwd,
                                     self.VERTI_port), autocommit=True)  # 自动提交
        except Exception as e:
            raise DataBaseConnectError('executing function "%s.vertica_engine" caught %s'% (self.__class__.__name__, e))

