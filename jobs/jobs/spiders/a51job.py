# -*- coding: utf-8 -*-
import scrapy
from jobs.items import JobsItem
import time
import sqlalchemy
import pandas as pd
import re
import urllib


class A51jobSpider(scrapy.Spider):
    name = '51job'
    allowed_domains = ['51job.com']



    def __init__(self):
        self.mydb = sqlalchemy.create_engine("mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8".format(
            scrapy.conf.settings['MYSQL_USER'], scrapy.conf.settings['MYSQL_PASSWORD'], scrapy.conf.settings['MYSQL_HOST'], scrapy.conf.settings['MYSQL_PORT'], scrapy.conf.settings['MYSQL_DATABASE']))

        sql = 'select * from {0} where crawl_pages = (select max(crawl_pages) from {0})'.format(self.name + scrapy.conf.settings['MYSQL_SUFFIX_CRAWLED'])
        df = pd.read_sql_query(sql,self.mydb)
        
        self.query_list = ['c%2B%2B', 'java', 'python', 'Hadoop', 'golang','html5', 'javascript', '机器学习', '图像处理', '机器视觉', '运维']
        self.city_list = ['010000', '020000', '030200','040000', '080200', '090200', '180200', '230200']
        self.date_time = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        
        a = urllib.parse.quote('c%2B%2B')


        if df.empty:
            self.page_count = 1
            self.query_count = 0
            self.city_count = 0
            self.total_page_count = 0

        else:
            crawled_url = df.loc[0,'crawl_url']
            crawled_pages = df.loc[0,'crawl_pages']

            self.page_count = int(re.search(r'(?<=,)\d+(?=.html)',urllib.parse.unquote(crawled_url)).group(0))
            self.query_count = int(self.query_list.index(re.search(r'(?<=9,99,)\S+(?=,2,)',urllib.parse.unquote(crawled_url)).group(0))) 
            self.city_count = int(self.city_list.index(re.search(r'(?<=list/)\S+(?=,000000,0000,00,9,99)',urllib.parse.unquote(crawled_url)).group(0)))
            self.total_page_count = int(crawled_pages) - 1
             
        self.start_urls = ['https://search.51job.com/list/{0},000000,0000,00,9,99,{1},2,{2}.html'.format(self.city_list[self.city_count], urllib.parse.quote(self.query_list[self.query_count]), self.page_count)]
        print('lnkyzhang',self.start_urls)
            



    def parse(self, response):
        self.page_count += 1
        self.total_page_count += 1
        print("lnkyzhang", self.total_page_count)

        if response.url.strip():

            groups = response.xpath('//div[@class="dw_table"]/div[@class="el"]')
            for each_group in groups:
                item = JobsItem()
                item['job_title'] = each_group.xpath(
                    './/a[@target="_blank"]/@title').extract()[0]
                
                try:
                    item['salary'] = each_group.xpath(
                        './/span[@class="t4"]/text()').extract()[0]
                except (TypeError, IndexError):
                    pass

                # item['experience'] = each_group.xpath(
                #     './/div[@class="info-primary"]/p/text()[2]').extract()[0]
                item['location'] = each_group.xpath(
                    './/span[@class="t3"]/text()').extract()[0].split()[0]
                item['detail_url'] = each_group.xpath(
                    './/a[@target="_blank"]/@href').extract()[0].split()[0]
                item['update_date'] = each_group.xpath(
                    './/span[@class="t5"]/text()').extract()[0]
                # item['welfare'] = ",".join(each_group['welfare'])
                item['key_word'] = self.query_list[self.query_count]
                item['company_title'] = each_group.xpath(
                    './/span[@class="t2"]/a/@title').extract()[0]

                item['crawl_date'] = self.date_time
                item['crawl_url'] = response.request.url
                item['crawl_pages'] = self.total_page_count
                # try:
                #     item['company_scale'] = each_group.xpath(
                #         './/div[@class="company-text"]/p/text()[3]').extract()[0]
                # except (TypeError, IndexError):
                #     pass
                # item['company_nature'] = each_group.xpath(
                #     './/div[@class="company-text"]/p/text()[2]').extract()[0]

                yield item

            if response.xpath('//li[@class="bk"][2]/span/text()'):
                if response.xpath('//li[@class="bk"][2]/span/text()').extract()[0] == '下一页':
                    self.page_count = 1

                    if self.query_count < len(self.query_list) - 1:
                        self.query_count += 1

                    elif self.city_count < len(self.city_list) - 1:
                        self.query_count = 0
                        self.city_count += 1

                

        url = 'https://search.51job.com/list/{0},000000,0000,00,9,99,{1},2,{2}.html'.format(self.city_list[self.city_count], urllib.parse.quote(self.query_list[self.query_count]), self.page_count)
        yield scrapy.Request(url=url, callback=self.parse)