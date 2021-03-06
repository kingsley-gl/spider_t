#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2018/03/26
# @Author  : kingsley kwong
# @Site    :
# @File    : tmall_spider.py
# @Software: tmall spider
# @Function:

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.keys import Keys
import time
import datetime
from util.proxy_pool import (firefox_with_proxy, load_proxy_2)
from util.exceptions import NoElementError
from base_state import (State, WorkState)
from util.logger import log
import re
TIMING = 2.0


global URL
global logger


class DetailState(State):
    '''抓取商品详情状态'''
    logger_name = 'spider_process'
    __name__ = 'Detail'

    def __init__(self,
                 primary_key,
                 good_iid,
                 url,
                 main_data_queue=None,
                 comment_data_queue=None):
        self.primary_key = primary_key
        self.good_iid = good_iid
        self.url = url
        self.fail_state = self
        self.back_state = self
        self.main_data_queue = main_data_queue
        self.comment_data_queue = comment_data_queue
        self.success_state = CommentState(primary_key=self.primary_key,
                                          good_iid=self.good_iid,
                                          url=self.url,
                                          comment_data_queue=self.comment_data_queue)
        self.parameters = {}
        self.crawl_posi_tags = {}
        self.crawl_neg_tags = {}
        self.crawl_main_outer_id = None
        # super(State, self).__init__()

    def do(self, driver):
        try:
            h2 = self.browser_operation(driver=driver, locate_way='find_element_by_xpath',
                                        xpath="//div[@id='content']/div/div/h2", operator='text')
            if u'很抱歉，您查看的商品找不到了！' in h2:
                return None
        except NoElementError:
            pass
        crawl_main_product_url = self.url
        crawl_main_title = driver.title  # title 标题
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
        main_data_pack = {'crawl_good_iid': self.good_iid, 'crawl_primary_key': str(self.primary_key)}
        for key in locals().keys():
            if 'crawl_' in key:
                main_data_pack.update({key: locals()[key]})
        main_data_pack.update({'crawl_prop_parameters': self.parameters})
        main_data_pack.update({'crawl_prop_posi_tag': self.crawl_posi_tags})
        main_data_pack.update({'crawl_prop_neg_tag': self.crawl_neg_tags})
        main_data_pack.update({'crawl_main_outer_id': self.crawl_main_outer_id})
        global logger
        # logger.info('main_data_pack %s' % main_data_pack)
        if self.main_data_queue is not None:
            self.main_data_queue.put(main_data_pack)  # 主数据包入队列
            self.main_data_queue.put('close')  # 传入关闭进程指令
        return self.success_state


class CommentState(State):
    '''抓取评论状态'''
    logger_name = 'spider_process'
    __name__ = 'Comment'

    def __init__(self,
                 primary_key,
                 good_iid,
                 url,
                 main_data_queue=None,
                 comment_data_queue=None):
        self.primary_key = primary_key
        self.good_iid = good_iid
        self.url = url
        self.success_state = None
        self.fail_state = self
        self.back_state = DetailState
        self.main_data_queue = main_data_queue
        self.comment_data_queue = comment_data_queue
        # super(State, self).__init__()

    def do(self, driver):
        global logger
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
                            eval_data_pack = {'crawl_good_iid': self.good_iid,
                                              'crawl_primary_key': str(self.primary_key),
                                              'crawl_page_id': ''.join(['%06d' % page, self.good_iid])}
                            for key in locals().keys():
                                if 'crawl_' in key:
                                    eval_data_pack.update({key: locals()[key]})

                            # logger.info('eval_data_pack %s' % eval_data_pack)
                            # self.data_queue.append(eval_data_pack)  # 评论数据包入队列
                            if self.comment_data_queue is not None:
                                self.comment_data_queue.put(eval_data_pack)  # 评论数据包入队列
                            self.browser_operation(driver=driver, locate_way='find_element_by_link_text',
                                                   xpath=u"下一页>>", operator='click')
                            page += 1
                    except NoElementError:
                        break
                tag_cnt += 1
            except NoElementError:
                break

        if self.comment_data_queue is not None:
            self.comment_data_queue.put('close')  # 传入关闭指令

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


def crawl_tmall_data(good_iid, main_data_queue, comment_data_queue, engine, platform):
    global logger
    url = r'https://detail.tmall.com/item.htm?id=%s' % good_iid
    logger = log.getLogger('spider_process')
    logger.info('iid(%s) spider start' % good_iid)
    conn = engine.vertica_engine()
    with conn.cursor() as crsr:
        crsr.execute('''SELECT id FROM huimei.dc_platform_products_main 
                        WHERE iid=%s AND platform=%s''' % (good_iid, platform))
        ret = crsr.fetchall()
        if len(ret) == 0:
            rowcount = crsr.execute('''INSERT INTO huimei.dc_platform_products_main (iid, platform)
                                          VALUES(%s, %s);''' % (good_iid, platform))
            if rowcount:

                crsr.execute('''SELECT s.id  FROM huimei.dc_platform_products_main s INNER JOIN
                                (SELECT iid, MAX(gmt_modified) AS maxgmt  FROM huimei.dc_platform_products_main  where iid=%s GROUP BY iid) a
                                ON s.iid=a.iid
                                where s.iid=%s
                                AND gmt_modified>=a.maxgmt''' % (good_iid, good_iid))
                ret = crsr.fetchall()
        primary_key = ret[0][0]
    conn.close()

    def call_proxy():
        try:
            proxy = load_proxy_2()
            b = firefox_with_proxy(proxy['host'], proxy['port'])
            b.set_page_load_timeout(120)
            b.get(r'https://detail.tmall.com/item.htm?id=%s' % good_iid)
            return b
        except TimeoutException:  # 超时换代理重试
            logger.warning('proxy host:%s port:%s TimeOut loading page.Retry another proxy'
                           % (proxy['host'], proxy['port']))
            b.close()
            return call_proxy()
    b = call_proxy()
    w = WorkState(driver=b, default_state=DetailState(primary_key,
                                                      good_iid,
                                                      url,
                                                      main_data_queue,
                                                      comment_data_queue))
    w.run()
    b.close()
    # logger.info('end spider_sales ')




