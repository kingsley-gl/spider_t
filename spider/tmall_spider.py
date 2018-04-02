#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2018/03/26
# @Author  : kingsley kwong
# @Site    :
# @File    : tmall_spider.py
# @Software: tmall spider
# @Function:

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
import datetime
from util.proxy_pool import (firefox_with_proxy, load_proxy_2)
import os
from util.exceptions import NoElementError
from base_state import (State, WorkState)
from util.logger import log
import re
TIMING = 2.0
global GOOD_IID
global PRIMARY_KEY
global URL
global logger


class DetailState(State):
    '''抓取商品详情状态'''
    logger_name = 'spider_process'
    __name__ = 'Detail'

    def __init__(self, main_data_queue, comment_data_queue):
        self.success_state = CommentState(comment_data_queue)
        self.fail_state = self
        self.data_queue = main_data_queue
        self.parameters = {}
        self.crawl_posi_tags = {}
        self.crawl_neg_tags = {}
        self.crawl_main_outer_id = None

    def do(self, driver):
        global URL
        crawl_main_product_url = URL
        crawl_main_title = driver.title # title 标题
        crawl_main_shop = self.browser_operation(driver=driver, locate_way='find_element_by_xpath',
                                             xpath="//a[@class='shopLink']", operator='text')
        crawl_main_shop_id = float(self.browser_operation(driver=driver, locate_way='find_element_by_xpath',
                               xpath="//div[@id='LineZing']", operator='get_attribute', key='shopid'))  # shop_id
        crawl_main_collect_num = self.browser_operation(driver=driver, locate_way='find_element_by_id',
                               xpath=r"J_CollectCount", operator='text')  # 人气收藏
        crawl_main_collect_num = float(re.compile(r'[0-9]\d*').findall(crawl_main_collect_num)[0])  # 人气收藏数 去多余字符
        crawl_price_price = {self.browser_operation(driver=driver, locate_way='find_element_by_xpath',
                               xpath=r"//div[@id='J_DetailMeta']/div/div/div/div[2]/dl/dt", operator='text'):  # 价格名
                             float(self.browser_operation(driver=driver, locate_way='find_element_by_xpath',
                               xpath=r"//div[@id='J_DetailMeta']/div/div/div/div[2]/dl/dd/span", operator='text'))}  # 价格
        crawl_main_rate_num = self.browser_operation(driver=driver, locate_way='find_element_by_xpath',
                               xpath="//ul[@id='J_TabBar']/li[2]", operator='text')  # 总评论数
        crawl_main_rate_num = float(re.compile(r'[0-9]\d*').findall(crawl_main_rate_num)[0])  # 总评论数 去多余字符
        if not self.parameters:
            i = 1
            while True:
                try:
                    goods_name = self.browser_operation(driver=driver, locate_way='find_element_by_xpath',
                                                        xpath="//ul[@id='J_AttrUL']/li[%s]" % i, operator='text')  # 商品详情
                    if u'货号:' in goods_name:
                        self.crawl_main_outer_id = re.sub('[^a-zA-Z0-9]*', '', goods_name)  # 货号
                        i += 1
                        continue

                    good_key = goods_name.split(u':')[0]
                    good_value = goods_name.split(u':')[1]
                    self.parameters.update({good_key: good_value})
                    i += 1
                except NoElementError:
                    break

        self.browser_operation(driver=driver, locate_way='find_element_by_xpath',
                               xpath="//ul[@id='J_TabBar']/li[2]", operator='click')  # 点击：评论
        crawl_main_dsr_qual_score_avg = float(self.browser_operation(driver=driver, locate_way='find_element_by_xpath',
                               xpath="//div[@id='J_Reviews']/div/div/div/strong", operator='text'))  # 与描述相符评分
        crawl_main_pic_path = (self.browser_operation(driver=driver, locate_way='find_element_by_xpath',
                               xpath="//img[@id='J_ImgBooth']", operator='get_attribute', key='src'))  # 首图url

        try:
            crawl_main_selled_count_last_month = float(self.browser_operation(driver=driver, locate_way='find_element_by_xpath',
                                                      xpath="//div[@id='J_DetailMeta']/div/div/div/ul/li/div/span[2]",
                                                      operator='text'))  # 月销量
        except NoElementError:  # 页面没有月销量
            pass
        try:
            crawl_price_promo_price = {self.browser_operation(driver=driver, locate_way='find_element_by_xpath',
                                                 xpath="//dl[@id='J_PromoPrice']/dt", operator='text'):  # 促销价格名（新风尚价格）
                                       float(self.browser_operation(driver=driver, locate_way='find_element_by_xpath',
                                                                    xpath="//dl[@id='J_PromoPrice']/dd/div/span",
                                                                    operator='text'))}  # 促销价格（新风尚价格）
        except NoElementError:  # 页面没有促销价格
            pass
        try:
            crawl_main_stock = self.browser_operation(driver=driver, locate_way='find_element_by_id',
                               xpath=r"J_EmStock", operator='text')  # 库存
            crawl_main_stock = float(re.compile(r'[0-9]\d*').findall(crawl_main_stock)[0])  # 总评论数 去多余字符
        except NoElementError:  # 页面没有库存
            pass
        try:
            self.browser_operation(driver=driver, locate_way='find_element_by_xpath',
                                   xpath="//div[@id='J_Reviews']/div/div/div[3]/span", operator='click')  # 点击标签下拉
            i = 1
            while True:  # 获取正面评论标签
                try:
                    posi_tag = self.browser_operation(driver=driver, locate_way='find_element_by_xpath',
                                                      xpath="//span[@class='tag-posi']["+str(i)+"]/a", operator='text')
                    pos_value = re.compile(r'[0-9]\d*').findall(posi_tag)[0]
                    pos_tag = posi_tag.replace('(%s)' % pos_value, '')
                    self.crawl_posi_tags.update({pos_tag: pos_value})
                    i += 1
                except NoElementError:
                    break
            j = 1
            while True:  # 获取负面评论标签
                try:
                    neg_tag = self.browser_operation(driver=driver, locate_way='find_element_by_xpath',
                                                     xpath="//span[@class='tag-neg']["+str(j)+"]/a", operator='text')
                    neg_value = re.compile(r'[0-9]\d*').findall(neg_tag)[0]
                    neg_tag = neg_tag.replace('(%s)' % neg_value, '')
                    self.crawl_neg_tags.update({neg_tag: neg_value})
                    j += 1
                except NoElementError:
                    break
        except NoElementError:  # 页面没有标签
            pass
        global GOOD_IID, PRIMARY_KEY
        main_data_pack = {'crawl_good_iid': GOOD_IID, 'crawl_primary_key': str(PRIMARY_KEY)}
        for key in locals().keys():
            if 'crawl_' in key:
                main_data_pack.update({key: locals()[key]})
        main_data_pack.update({'crawl_prop_parameters':self.parameters})
        main_data_pack.update({'crawl_prop_posi_tag':self.crawl_posi_tags})
        main_data_pack.update({'crawl_prop_neg_tag':self.crawl_neg_tags})
        main_data_pack.update({'crawl_main_outer_id':self.crawl_main_outer_id})
        global logger
        # logger.info('main_data_pack %s'%main_data_pack)
        self.data_queue.append(main_data_pack)  # 主数据包入队列
        self.data_queue.append('close')  # 传入关闭进程指令
        return self.success_state

class CommentState(State):
    '''抓取评论状态'''
    logger_name = 'spider_process'
    __name__ = 'Comment'

    def __init__(self, data_queue):
        self.success_state = None
        self.fail_state = self
        self.data_queue = data_queue

    def do(self, driver):
        global logger
        global GOOD_IID, PRIMARY_KEY
        tag_cnt = 1  # 标签点击计数器
        while True:  # 点击负面标签循环
            try:
                self.browser_operation(driver=driver, locate_way='find_element_by_xpath',
                                       xpath="//span[@class='tag-neg'][" + str(tag_cnt) + "]/a", operator='click')
                selected = self.browser_operation(driver=driver, locate_way='find_element_by_xpath',
                                                  xpath="//span[@class='tag-neg'][" + str(tag_cnt) + "]/a",
                                                  operator='get_attribute', key='class')
                # logger.info('neg selected:%s' % selected)
                time.sleep(1.0)
                if selected not in 'selected':
                    raise NoElementError
                page = 1
                while True:  # 翻页循环
                    try:
                        i = 1   # floor
                        crawl_eval_page_pack = []
                        crawl_img_page_pack = []
                        while True:  # 单页评论循坏
                            try:
                                comment_flag = False  # 评论判断标志，有初评时，必然找不到评论
                                commt_premiere_rate_content = None
                                commt_premiere_rate_date = None
                                commt_append_rate_content = None
                                commt_premiere_rate_reply = None
                                commt_append_rate_reply = None
                                src_premiere_img_src_path = []
                                src_append_img_src_path = []
                                try:
                                    commt_premiere_rate_content = self.browser_operation(driver=driver, locate_way='find_element_by_xpath',
                                                           xpath="//div[@id='J_Reviews']/div/div[6]/table/tbody/tr["+str(i)+"]/td[1]/div[@class='tm-rate-premiere']/div[@class='tm-rate-content']/div[@class='tm-rate-fulltxt']",
                                                           operator='text')  # 初评
                                    commt_premiere_rate_date = self.browser_operation(driver=driver, locate_way='find_element_by_xpath',
                                                                 xpath="//div[@id='J_Reviews']/div/div[6]/table/tbody/tr["+str(i)+"]/td[1]/div[@class='tm-rate-premiere']/div[@class='tm-rate-tag']/div[@class='tm-rate-date']",
                                                                 operator='text')  # 初评日期
                                    commt_append_rate_content = self.browser_operation(driver=driver, locate_way='find_element_by_xpath',
                                                                 xpath="//div[@id='J_Reviews']/div/div[6]/table/tbody/tr["+str(i)+"]/td[1]/div[@class='tm-rate-append']/div[@class='tm-rate-content']/div[@class='tm-rate-fulltxt']",
                                                                 operator='text')  # 追评
                                    li = 1
                                    while True:
                                        try:
                                            src_premiere_img_src_path.append(
                                                self.browser_operation(driver=driver,
                                                                       locate_way='find_element_by_xpath',
                                                                       xpath="//div[@id='J_Reviews']/div/div[6]/table/tbody/tr["+str(i)+"]/td[1]/div[@class='tm-rate-premiere']/div[@class='tm-rate-content']/div[@class='tm-m-photos']/ul/li["+str(li)+"]/img",
                                                                       operator='text'))  # 初评买家秀
                                            li += 1
                                        except NoElementError:
                                            # logger.info(src_premiere_img_src_path)
                                            break
                                    li = 1
                                    while True:
                                        try:
                                            src_append_img_src_path.append(
                                                self.browser_operation(driver=driver,
                                                                       locate_way='find_element_by_xpath',
                                                                       xpath="//div[@id='J_Reviews']/div/div[6]/table/tbody/tr["+str(i)+"]/td[1]/div[@class='tm-rate-append']/div[@class='tm-rate-content']/div[@class='tm-m-photos']/ul/li["+str(li)+"]/img",
                                                                       operator='get_attribute',
                                                                       key='src'))  # 追评买家秀
                                            li += 1
                                        except NoElementError:
                                            # logger.info(src_append_img_src_path)
                                            break
                                except NoElementError:
                                    comment_flag = True
                                    commt_premiere_rate_content = None
                                    commt_premiere_rate_date = None
                                    pass
                                try:
                                    commt_premiere_rate_reply = self.browser_operation(driver=driver, locate_way='find_element_by_xpath',
                                                           xpath="//div[@id='J_Reviews']/div/div[6]/table/tbody/tr["+str(i)+"]/td[1]/div[@class='tm-rate-premiere']/div[@class='tm-rate-reply']/div[@class='tm-rate-fulltxt']",
                                                           operator='text')  # 初解释

                                except NoElementError:
                                    commt_premiere_rate_reply = None
                                    pass
                                try:
                                    commt_append_rate_reply = self.browser_operation(driver=driver, locate_way='find_element_by_xpath',
                                                           xpath="//div[@id='J_Reviews']/div/div[6]/table/tbody/tr["+str(i)+"]/td[1]/div[@class='tm-rate-append']/div[@class='tm-rate-reply']/div[@class='tm-rate-fulltxt']",
                                                           operator='text')  # 追评解释

                                except NoElementError:
                                    commt_append_rate_reply = None
                                    pass
                                if comment_flag:
                                    commt_premiere_rate_content = self.browser_operation(driver=driver, locate_way='find_element_by_xpath',
                                                           xpath="//div[@id='J_Reviews']/div/div[6]/table/tbody/tr["+str(i)+"]/td/div/div",
                                                           operator='text')  # 评论
                                    commt_premiere_rate_date = self.browser_operation(driver=driver, locate_way='find_element_by_xpath',
                                                           xpath="//div[@id='J_Reviews']/div/div[6]/table/tbody/tr["+str(i)+"]/td/div[2]",
                                                           operator='text')  # 评论日期
                                    li = 1
                                    while True:
                                        try:
                                            src_premiere_img_src_path.append(
                                                self.browser_operation(driver=driver,
                                                                       locate_way='find_element_by_xpath',
                                                                       xpath="//div[@id='J_Reviews']/div/div[6]/table/tbody/tr["+str(i)+"]/td/div/div[@class='tm-m-photos']/ul/li["+str(li)+"]/img",
                                                                       operator='get_attribute',
                                                                       key='src'))  # 评论买家秀
                                            li += 1
                                        except NoElementError:
                                            # logger.info(src_premiere_img_src_path)
                                            break
                                    try:
                                        commt_premiere_rate_reply = self.browser_operation(driver=driver, locate_way='find_element_by_xpath',
                                                           xpath="//div[@id='J_Reviews']/div/div[6]/table/tbody/tr["+str(i)+"]/td[1]/div[@class='tm-rate-reply']/div[@class='tm-rate-fulltxt']",
                                                           operator='text')  # 评论解释
                                    except NoElementError:
                                        commt_premiere_rate_reply = None
                                        pass
                                commt_rate_prop1 = self.browser_operation(driver=driver, locate_way='find_element_by_xpath',
                                                       xpath="//div[@id='J_Reviews']/div/div[6]/table/tbody/tr["+str(i)+"]/td[2]/div/p[1]",
                                                       operator='get_attribute', key='title')  # 评论属性1 （颜色分类）
                                commt_rate_prop2 = self.browser_operation(driver=driver, locate_way='find_element_by_xpath',
                                                       xpath="//div[@id='J_Reviews']/div/div[6]/table/tbody/tr["+str(i)+"]/td[2]/div/p[2]",
                                                       operator='get_attribute', key='title')  # 评论属性2 （尺码）
                                commt_buyer_nick = self.browser_operation(driver=driver, locate_way='find_element_by_xpath',
                                                       xpath="//div[@id='J_Reviews']/div/div[6]/table/tbody/tr["+str(i)+"]/td[3]/div",
                                                       operator='text')
                                # logger.info(
                                #     'iid: %s page: %s, floor: %s 初评:%s' % (GOOD_IID, page, i, commt_premiere_rate_content))
                                # logger.info(
                                #     'iid: %s page: %s, floor: %s 初评日期:%s' % (GOOD_IID, page, i, commt_premiere_rate_date))
                                # logger.info(
                                #     'iid: %s page: %s, floor: %s 追评:%s' % (GOOD_IID, page, i, commt_append_rate_content))
                                # logger.info(
                                #     'iid: %s page: %s, floor: %s 初解释:%s' % (GOOD_IID, page, i, commt_premiere_rate_reply))
                                # logger.info(
                                #     'iid: %s page: %s, floor: %s 追评解释:%s' % (GOOD_IID, page, i, commt_append_rate_reply))
                                eval_floor_pack = {'floor': i}
                                img_floor_pack = {'floor': i}
                                i += 1
                                for e_key in locals().keys():
                                    if 'commt_' in e_key:
                                        eval_floor_pack.update({e_key: locals()[e_key]})
                                    elif 'src_' in e_key:
                                        img_floor_pack.update({e_key: locals()[e_key]})
                                crawl_eval_page_pack.append(eval_floor_pack)
                                crawl_img_page_pack.append(img_floor_pack)
                            except NoElementError:
                                break
                        if crawl_eval_page_pack:    # 防空包
                            # global GOOD_IID, PRIMARY_KEY
                            eval_data_pack = {'crawl_good_iid': GOOD_IID,
                                              'crawl_primary_key': str(PRIMARY_KEY),
                                              'crawl_page_id': ''.join(['%06d' % page, GOOD_IID])}
                            for key in locals().keys():
                                if 'crawl_' in key:
                                    eval_data_pack.update({key: locals()[key]})

                            # logger.info('eval_data_pack %s' % eval_data_pack)
                            self.data_queue.append(eval_data_pack)  # 评论数据包入队列
                            self.browser_operation(driver=driver, locate_way='find_element_by_link_text',
                                                   xpath=u"下一页>>", operator='click')
                            page += 1
                    except NoElementError:
                        break
                tag_cnt += 1
            except NoElementError:
                break
        self.data_queue.append('close')  # 传入关闭指令


        return self.success_state








def crawler_days(crawl_days, crawl_dates):
    '''
    计算爬取日期
    :param crawl_days: 爬取天数
    :param crawl_dates: 爬取日期
    :return: 所有的爬取的日期
    '''
    today = datetime.date.today()
    str2date = lambda x:datetime.datetime.strptime(x, "%Y-%m-%d").date()
    date2str = lambda x:x.strftime("%Y-%m-%d")
    if crawl_days != 0:
        date_of_days = [today - datetime.timedelta(days=days) for days in range(1,crawl_days+1)]
        return set(map(date2str, date_of_days) + crawl_dates)
    else:
        return crawl_dates


def crawl_tmall_data(good_iid, main_data_queue, comment_data_queue, engine):
    global logger
    logger = log.getLogger('spider_process')
    logger.info('iid(%s) spider start' % good_iid)
    conn = engine.vertica_engine()
    with conn.cursor() as crsr:
        rowcount = crsr.execute('''INSERT INTO huimei.dc_platform_products_main (iid)
                                      VALUES(%s);'''%good_iid)
        if rowcount:
            crsr.execute('''SELECT s.id  FROM huimei.dc_platform_products_main s INNER JOIN 
                            (SELECT iid, MAX(gmt_modified) AS maxgmt  FROM huimei.dc_platform_products_main  where iid=%s GROUP BY iid) a 
                            ON s.iid=a.iid 
                            where s.iid=%s 
                            AND gmt_modified>=a.maxgmt''' % (good_iid,good_iid))
            global PRIMARY_KEY
            PRIMARY_KEY = crsr.fetchall()[0][0]
    conn.close()
    proxy = load_proxy_2()
    b = firefox_with_proxy(proxy['host'], proxy['port'])
    b.set_page_load_timeout(120)
    global URL
    URL = r'https://detail.tmall.com/item.htm?id=%s' % good_iid
    b.get(r'https://detail.tmall.com/item.htm?id=%s' % good_iid)
    global GOOD_IID
    GOOD_IID = good_iid
    w = WorkState(driver=b, default_state=DetailState(main_data_queue, comment_data_queue))
    w.run()
    b.close()
    # logger.info('end spider_sales ')


# def download_and_process(user_id, shop, login_user,
#                          password, rawFileSavePath,
#                          csvSaveRootPath,crawlDays,crawlDates):
#
#     rawFileSavePath = rawFileSavePath + '\\sale'
#     if not os.path.exists(rawFileSavePath):
#         os.makedirs(rawFileSavePath)
#     profile = webdriver.FirefoxProfile()
#     profile.set_preference("browser.download.folderList", 2)
#     profile.set_preference("browser.download.dir", rawFileSavePath)
#     profile.set_preference("browser.helperApps.neverAsk.saveToDisk",
#                            "application/octet-stream, application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
#     profile.set_preference("browser.download.useDownloadDir", True)
#     profile.update_preferences()
#     b = webdriver.Firefox(firefox_profile=profile)
#     b.set_page_load_timeout(30)
#
#     b.get(r'https://vis.vip.com/login.php')
#     while True:
#         # 登录
#         try:
#             b.find_element_by_xpath(r'//*[@id="userName"]').send_keys(login_user)
#             b.find_element_by_xpath(r'//*[@id="passWord"]').send_keys(password)
#             b.find_element_by_xpath(r'//*[@id="checkWord"]').send_keys('')
#             break
#         except:pass
#     while True:
#         try:
#             b.find_element_by_xpath(r'//li[@data-v-0944691c]').click()  # 点击：魔方罗盘
#             time.sleep(TIMING)
#             break
#         except Exception as e:
#             time.sleep(3)
#     #切换窗口
#     root_handle = b.current_window_handle
#     b.switch_to.window(root_handle)
#     b.close()
#     for handle in b.window_handles:
#         if handle != root_handle:
#             b.switch_to.window(handle)
#             break
#     root_url = b.current_url
#     # 记录所有档期
#     time.sleep(3)
#     b.find_element_by_xpath(r'//button[@aria-label="Close"]').click()  # 点击：新功能导航页面关闭
#     while True:
#         try:
#             time.sleep(TIMING)
#             b.find_element_by_xpath("//div[@id='compass-app-body']/div/div/div/ul/li[4]/div").click()  # 点击：左栏第一级 商品
#             time.sleep(TIMING)
#             b.find_element_by_xpath("//div[@id='compass-app-body']/div/div/div/ul/li[4]/ul/li").click()  # 点击：左栏第二级 商品详情
#             time.sleep(TIMING)
#             b.find_element_by_xpath(
#                 "//div[@id='compass-app-body']/div[2]/div/div[2]/div/label[2]/span/span").click()   #点击：档期圆点
#             time.sleep(TIMING)
#             b.find_element_by_xpath(
#                 "//div[@id='compass-app-body']/div[2]/div/div[3]/div/div/div/div/div[2]/div/div/label/span/span").click()   #点击：全部方框
#             time.sleep(TIMING)
#             b.find_element_by_xpath(
#                 "//div[@id='compass-app-body']/div[2]/div/div[3]/div/div/div/div/div[3]/div/div/div/label/span/span").click()   #点击：条形码方框
#             time.sleep(TIMING)
#
#             break
#         except Exception as e:
#             print 'Exception is %s'%e
#             b.refresh()
#             b.get(root_url)
#             time.sleep(5)
#
#     i = 1
#     brands = list()
#     b.find_element_by_xpath("//input[@type='text']").click()    #点击：品牌下拉
#     time.sleep(2)
#     while True:
#         try:
#             brands.append(b.find_element_by_xpath("//div[4]/div/div/ul/li["+str(i)+"]/span").text)  #获取所有品牌
#             i += 1
#         except Exception as e:
#             print('ID-%d brand Exception %s' % (i, e))
#             b.find_element_by_xpath("//input[@type='text']").click()  # 点击：品牌下拉
#             break
#
#
#     def sleep_decorator(*dargs,**dkwargs):
#         def _wraper(func):
#             def _inner_wrapper(*args,**kwargs):
#                 func(*args,**kwargs)
#                 time.sleep(dkwargs['time'])
#             return _inner_wrapper
#         return _wraper
#
#     @sleep_decorator(time=TIMING)
#     def send_dangqi_keys(dangqi):
#         '''
#         发送档期值，下拉点击
#         :param dangqi:
#         :return:
#         '''
#         if dangqi:
#             b.find_element_by_xpath("(//input[@type='text'])[2]").click()  # 点击：档期下拉
#             time.sleep(TIMING)
#             print(dangqi)
#             b.find_element_by_xpath("(//input[@type='text'])[2]").send_keys(dangqi)  # 档期值输入
#             time.sleep(TIMING)
#             try:
#                 b.find_element_by_xpath("(//input[@type='text'])[2]").send_keys(Keys.DOWN)  # 键盘操作：下
#                 time.sleep(TIMING)
#                 b.find_element_by_xpath("(//input[@type='text'])[2]").send_keys(Keys.ENTER)  # 键盘操作：回车
#                 time.sleep(TIMING)
#                 dangqi_period_date.append({dangqi:b.find_element_by_xpath(
#                     "//div[@id='compass-app-body']/div[2]/div/div[2]/div/div/div[2]/span[2]/span[2]").text})
#                 time.sleep(TIMING)
#                 b.find_element_by_xpath("(//button[@type='button'])[2]").click()
#                 time.sleep(TIMING)
#             except Exception as e:
#                 print('档期-%s Exception is %s'%(dangqi,e))
#                 pass
#
#     def crawler_days(crawl_days,crawl_dates):
#         '''
#         计算爬取日期
#         :param crawl_days: 爬取天数
#         :param crawl_dates: 爬取日期
#         :return: 所有的爬取的日期
#         '''
#         today = datetime.date.today()
#         str2date = lambda x:datetime.datetime.strptime(x, "%Y-%m-%d").date()
#         date2str = lambda x:x.strftime("%Y-%m-%d")
#         if crawl_days != 0:
#             date_of_days = [today - datetime.timedelta(days=days) for days in range(1,crawl_days+1)]
#             return set(map(date2str,date_of_days) + crawl_dates)
#         else:
#             return crawl_dates
#
#     crawlDates = crawler_days(crawlDays, crawlDates)
#     print('craw dates:',crawlDates)
#     all_of_dangqi = set()
#     for i,brand in enumerate(brands):
#         # print('brand %s'%brand)
#         try:
#             b.find_element_by_xpath("//input[@type='text']").click()
#             b.find_element_by_xpath("//input[@type='text']").send_keys(brand)   # 品牌栏输入品牌
#             time.sleep(TIMING)
#             if i != 0:
#                 b.find_element_by_xpath("//input[@type='text']").send_keys(Keys.DOWN)   #键盘操作：下
#                 b.find_element_by_xpath("//input[@type='text']").send_keys(Keys.ENTER)  #键盘操作：回车
#                 time.sleep(TIMING)
#         except:pass
#
#         j = 1
#         b.find_element_by_xpath("(//input[@type='text'])[2]").click()  # 点击：档期下拉
#         time.sleep(TIMING)
#         options = list()    #档期选项初始化，清空选项
#         while True:
#             try:
#                 options.append(b.find_element_by_xpath("//div[5]/div/div/ul/li[" + str(j) + "]/span").text)
#                 j += 1
#             except Exception as e:
#                 traceback.print_exc()
#                 print('ID-%d dangqi Exception %s'%(j,e))
#                 b.find_element_by_xpath("(//input[@type='text'])[2]").click()  # 点击：档期下拉
#                 break
#
#
#         if crawlDates:  # 指定日期
#             for crawlDate in crawlDates:    # 按日期爬取
#                 crawlDate = crawlDate.replace('-','')
#                 date_of_dangqi = set(map(lambda x:x if crawlDate in x else None,options))   # 指定日期档期
#                 # print(date_of_dangqi)
#                 all_of_dangqi.update(date_of_dangqi)
#                 map(send_dangqi_keys,date_of_dangqi)
#         else:
#             raise Exception("请指定一个可用的爬取日期")
#             break
#
#     print('sales download end')
#
#     delete_duplicate_files(rawFileSavePath)  # 删除重复下载文件
#     b.close()


