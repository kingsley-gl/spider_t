#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2018/03/19
# @Author  : kingsley kwong
# @Site    :
# @File    : logger.py
# @Software: tmall spider
# @Function:

import logging.config
import ConfigParser

config = ConfigParser.ConfigParser()
config.read('tmall.cfg')


LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'debug': {
            'format': '[LOGNAME-%(name)s][TIME-%(asctime)s] %(levelname)s: %(message)s'
        },
        'info': {
            'format': '[%(asctime)s][%(filename)s] %(levelname)s %(message)s'
        },
    },
    'handlers': {
        'null': {
            'level':'DEBUG',
            'class':'logging.NullHandler',
            'formatter':'debug',
        },
        'console_debug':{
            'level':'DEBUG',
            'class':'logging.StreamHandler',
            'formatter': 'debug',
            'stream':'ext://sys.stdout',
        },
        'console_info':{
            'level':'INFO',
            'class':'logging.StreamHandler',
            'formatter':'info',
            'stream':'ext://sys.stdout',
        },
        'file':{
            'level':'INFO',
            'class':'logging.FileHandler',
            'formatter':'info',
            'filename':'e:\\vip_spider\\log\\vip_spider.log',
        },
        'file_spider': {
            'level': 'INFO',
            'class': 'logging.FileHandler',
            'formatter': 'info',
            # 'filename': 'e:\\tmall_spider\\log\\spider.log',
            'filename': config.get('SPIDER_LOG', 'log_file_path'),
        },
        'file_database': {
            'level': 'INFO',
            'class': 'logging.FileHandler',
            'formatter': 'info',
            # 'filename': 'e:\\tmall_spider\\log\\database.log',
            'filename': config.get('DATABASE_LOG', 'log_file_path'),
        }
    },
    'loggers': {
        'spider_process': {
            'handlers': ['console_debug', 'file_spider'],
            'propagate': True,
            'level':'INFO',
        },
        'database_process': {
            'handlers': ['console_debug', 'file_database'],
            'propagate': True,
            'level': 'INFO',
        },
        # 'spider_info':{
        #     'handlers': ['file', 'console_info'],
        #     'propagate': True,
        #     'level': 'INFO',
        # }


    }
}

logging.config.dictConfig(LOGGING)
log = logging