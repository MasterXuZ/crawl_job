# -*- coding: utf-8 -*-
import scrapy
from jobs.items import JobsItem
import time
import sqlalchemy
import pandas as pd
import re
import urllib


class ZhipinSpider(scrapy.Spider):

    name = 'zhipin'
    allowed_domains = ['zhipin.com']

    def __init__(self):
        self.mydb = sqlalchemy.create_engine("mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8".format(
            scrapy.conf.settings['MYSQL_USER'], scrapy.conf.settings['MYSQL_PASSWORD'], scrapy.conf.settings['MYSQL_HOST'], scrapy.conf.settings['MYSQL_PORT'], scrapy.conf.settings['MYSQL_DATABASE']))

        sql = 'select * from {0} where crawl_pages = (select max(crawl_pages) from {0})'.format(self.name + scrapy.conf.settings['MYSQL_SUFFIX_CRAWLED'])
        df = pd.read_sql_query(sql,self.mydb)
        
        self.query_list = ['c%2B%2B', 'java', 'python', 'Hadoop', 'golang','html5', 'javascript', '机器学习', '图像处理', '机器视觉', '运维']
        self.city_list = ['c101010100', 'c101020100', 'c101280100','c101280600', 'c101210100', 'c101270100', 'c101200100', 'c101070100']
        self.date_time = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        
        


        if df.empty:
            self.page_count = 0
            self.query_count = 0
            self.city_count = 0
            self.total_page_count = 0

        else:
            crawled_url = df.loc[0,'crawl_url']
            crawled_pages = df.loc[0,'crawl_pages']

            self.page_count = int(re.search(r'(?<=page=)\d+',urllib.parse.unquote(crawled_url)).group(0))
            self.query_count = int(self.query_list.index(re.search(r'(?<=query=)\S+(?=&)',urllib.parse.unquote(crawled_url)).group(0))) 
            self.city_count = int(self.city_list.index(re.search(r'(?<=com/)\S+(?=/)',urllib.parse.unquote(crawled_url)).group(0)))
            self.total_page_count = int(crawled_pages) - 1
             
        self.start_urls = ['https://www.zhipin.com/{}/?query={}&page={}'.format(self.city_list[self.city_count], self.query_list[self.query_count], self.page_count)]
        print('lnkyzhang',self.start_urls)
            





    def parse(self, response):
        self.page_count += 1
        self.total_page_count += 1
        print("lnkyzhang", self.total_page_count)

        if response.url.strip():

            groups = response.xpath('//div[@class="job-list"]/ul/li')
            for each_group in groups:
                item = JobsItem()
                item['job_title'] = each_group.xpath(
                    './/div[@class="job-title"]/text()').extract()[0]
                item['salary'] = each_group.xpath(
                    './/span[@class="red"]/text()').extract()[0]
                item['experience'] = each_group.xpath(
                    './/div[@class="info-primary"]/p/text()[2]').extract()[0]
                item['location'] = each_group.xpath(
                    './/div[@class="info-primary"]/p/text()[1]').extract()[0].split()[0]
                item['detail_url'] = each_group.xpath(
                    './/h3[@class="name"]/a/@href').extract()[0].split()[0]
                item['update_date'] = each_group.xpath(
                    './/div[@class="info-publis"]/p/text()').extract()[0]
                #item['welfare'] = ",".join(each_group['welfare'])
                item['key_word'] = self.query_list[self.query_count]
                item['company_title'] = each_group.xpath(
                    './/div[@class="company-text"]/h3[@class="name"]/a/text()').extract()[0]

                item['crawl_date'] = self.date_time
                item['crawl_url'] = response.request.url
                item['crawl_pages'] = self.total_page_count
                try:
                    item['company_scale'] = each_group.xpath(
                        './/div[@class="company-text"]/p/text()[3]').extract()[0]
                except (TypeError, IndexError):
                    pass
                item['company_nature'] = each_group.xpath(
                    './/div[@class="company-text"]/p/text()[2]').extract()[0]

                yield item

            if response.xpath('//div[@class="page"]/a[@ka="page-next"]/@class'):
                if response.xpath('//div[@class="page"]/a[@ka="page-next"]/@class').extract()[0] == 'next':
                    pass
                else:
                    self.page_count = 0

                    if self.query_count < len(self.query_list) - 1:
                        self.query_count += 1

                    elif self.city_count < len(self.city_list) - 1:
                        self.query_count = 0
                        self.city_count += 1
            else:
                self.page_count = 0

                if self.query_count < len(self.query_list) - 1:
                    self.query_count += 1

                elif self.city_count < len(self.city_list) - 1:
                    self.query_count = 0
                    self.city_count += 1
                

        url = 'https://www.zhipin.com/{}/?query={}&page={}'.format(
            self.city_list[self.city_count], self.query_list[self.query_count], self.page_count)
        yield scrapy.Request(url=url, callback=self.parse)


    