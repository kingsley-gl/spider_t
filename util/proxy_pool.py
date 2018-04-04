#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2018/03/19
# @Author  : kingsley kwong
# @Site    :
# @File    : proxy_pool.py
# @Software: tmall spider
# @Function:

import sys
import random
from selenium import webdriver
from selenium.webdriver.common.by import By as by
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
import pickle
import os

# utf-8
reload(sys)
sys.setdefaultencoding('utf8')

def firefox_with_proxy(host = None, port = None, extension=None, disableJS=None):

    if host == None or port == None or host == '' or port == '':
        return False

    profile = webdriver.FirefoxProfile()
    profile.set_preference("network.proxy.type", 1)
    profile.set_preference("network.proxy.http", host)
    profile.set_preference("network.proxy.http_port", int(port))
    profile.set_preference("general.useragent.override","Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36")
    try:
        if len(str(extension)) > 0:
            # Value 1 : on
            # Value 2 : off
            profile.add_extension(str(extension))
            profile.set_preference("thatoneguydotnet.QuickJava.curVersion", "2.0.8")                ## Prevents loading the 'thank you for installing screen'
            profile.set_preference("thatoneguydotnet.QuickJava.startupStatus.Images", 2)            ## Turns images off
            profile.set_preference("thatoneguydotnet.QuickJava.startupStatus.CSS", 2)               ## CSS
            profile.set_preference("thatoneguydotnet.QuickJava.startupStatus.AnimatedImage", 2)     ## Turns animated images off
            profile.set_preference("thatoneguydotnet.QuickJava.startupStatus.Cookies", 2)           ## Cookies
            profile.set_preference("thatoneguydotnet.QuickJava.startupStatus.Flash", 2)             ## Flash
            profile.set_preference("thatoneguydotnet.QuickJava.startupStatus.Java", 2)              ## Java
            profile.set_preference("thatoneguydotnet.QuickJava.startupStatus.Silverlight", 2)       ## Silverlight

            # 默认不禁JS，因绝大多数页面内容由JS加载
            if disableJS == 'y':
                profile.set_preference("thatoneguydotnet.QuickJava.startupStatus.JavaScript", 2)    ## JavaScript
    except:
        pass

    profile.update_preferences()
    # ff = FirefoxBinary(r'C:\Program Files (x86)\Mozilla Firefox\firefox.exe')
    driver = webdriver.Firefox(firefox_profile=profile)  #,firefox_binary=ff)
    return driver

def load_proxy():

    driver = webdriver.PhantomJS(executable_path="phantomjs.exe")
    # driver = webdriver.Firefox()
    driver.set_page_load_timeout(1)
    try:
        driver.get('http://www.mimiip.com/gngao/%s'% str(random.randint(100, 200)))
    except:
        pass
    try:
        wait(driver,120).until(ec.presence_of_all_elements_located((by.XPATH, "//div[@id='mimiip']//tbody[1]//tr")))
        trs = driver.find_elements_by_xpath("//div[@id='mimiip']//tbody[1]//tr")
        for tr in trs:
            try:
                tds = tr.find_elements_by_tag_name('td')
                proxyType = tds[4].text
                anonymity = tds[3].text
                speed = int(extractText(tds[5].find_element_by_tag_name('div').get_attribute('style'), "width:", "px"))
                if proxyType[0:4] == 'HTTP' and anonymity == '高匿' and speed == 100:
                    host = tds[0].text
                    port = tds[1].text
                    return {'host' : host , 'port' : port}
            except:
                pass
    except:
        pass
    driver.quit()
    del driver

def load_proxy_1():

    driver = webdriver.PhantomJS(executable_path="phantomjs.exe")
    # driver = webdriver.Firefox()
    driver.set_page_load_timeout(1)
    try:
        driver.get('http://www.kuaidaili.com/free/inha/%s' % str(random.randint(1, 500)))
    except:
        pass
    try:
        trs = driver.find_elements_by_xpath("//tbody[1]//tr")
        for tr in trs:
            try:
                tds = tr.find_elements_by_tag_name('td')
                if '高匿名' in tds[2].text and str(tds[5].text)[:1] == '0':
                    host = tds[0].text
                    port = tds[1].text
                    return {'host' : host , 'port' : port}
            except:
                pass
    except:
        pass
    driver.quit()
    del driver

def load_proxy_2():

    proxyDictFile = os.getcwd() + '\\util\\proxyDict.data'  # 这个代理字典需要定期维护更新，结构为 {"num" : "xxx.xxx.xxx.xxx:xxxx", ... }
                                                            #  通过google、百度，whatever途径获得
    f = open(proxyDictFile, 'r')
    proxyDict = pickle.load(f)
    f.close()
    proxy = proxyDict[str(random.randint(1, proxyDict.__len__()))]  # 随机选一个
    host = proxy[:proxy.find(':')]
    port = proxy[proxy.find(':') + 1 :]
    return {'host' : host , 'port' : port}


def extractText(text, bgnText, endText, toBeReplacedByContent = None):
    '''
    Examples
        text = "233.0.0.jBruQt#/?brandId=86206958&cateId=5"

        If we want to extract "86206958" within "233.0.0.jBruQt#/?brandId=86206958&cateId=5" ,
        extractText(text, "brandId=", "&")
        :return: 86206958

        If we want to replace "86206958" with 'lalala'
        extractText(text, "brandId=", "&", 'lalala')
        :return: "233.0.0.jBruQt#/?brandId=lalala&cateId=5"
    '''
    if bgnText != '':
        bgnPos = text.find(bgnText)
    else:
        bgnPos = 0
    if endText != '':
        endPos = text.find(endText, bgnPos)
    else:
        endPos = len(text)

    if toBeReplacedByContent == None:
        return text[bgnPos + len(bgnText):endPos]
    else:
        return text[:bgnPos + len(bgnText)] + str(toBeReplacedByContent) + text[endPos:]
