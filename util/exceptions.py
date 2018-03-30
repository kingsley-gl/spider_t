#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2018/03/19
# @Author  : kingsley kwong
# @Site    :
# @File    : exception.py
# @Software: tmall spider
# @Function:
import sys

class BaseException(Exception):
    def __init__(self,err=''):
        err_msg = "%s at Line %d:%s in %s"%(self.__class__.__name__,sys.exc_info()[2].tb_lineno,
                                         err,
                                         sys.exc_info()[2].tb_frame.f_code.co_filename)
        Exception.__init__(self,err_msg)

class DataBaseConnectError(BaseException):pass
class DataBaseExecuteError(BaseException):pass
class NoElementError(BaseException):pass