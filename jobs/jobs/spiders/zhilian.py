# -*- coding: utf-8 -*-
import scrapy
from jobs.items import JobsItem
import json
import re
import time

class ZhilianSpider(scrapy.Spider):

    page_count = 0
    query_count = 0
    city_count = 0
    query_list = ['c%2B%2B','java','python','Hadoop','golang','html5','javascript','机器学习','图像处理','机器视觉','运维']
    city_list = ['530','538','763','765','653','801','736','599']

    name = 'zhilian'
    allowed_domains = ['zhaopin.com']
    start_urls = ['https://fe-api.zhaopin.com/c/i/sou?start={}&pageSize=90&cityId={}&workExperience=-1&education=-1&companyType=-1&employmentType=-1&jobWelfareTag=-1&kw={}&kt=3&_v=0.34053159&x-zp-page-request-id=d1dd66ee655347939b69acf87870ccc1-1548983897497-953047'.format(0,city_list[city_count],query_list[query_count])]
    date_time = time.strftime('%Y-%m-%d',time.localtime(time.time()))

    

    def parse(self, response):

        data = json.loads(response.text)

        if len(data['data']['results']) > 0:

            self.page_count += 1

            for each_group in data['data']['results']:
                item = JobsItem()
                item['job_title'] = each_group['jobName']
                item['salary'] = each_group['salary']
                item['experience'] = each_group['workingExp']['name']
                item['location'] = each_group['city']['items'][0]['name']
                item['detail_url'] = each_group['positionURL']
                item['update_date'] = each_group['updateDate'].split()[0]
                item['welfare'] = ",".join(each_group['welfare'])
                item['key_word'] = self.query_list[self.query_count]
                item['company_title'] = each_group['company']['name']
                item['company_scale'] = each_group['company']['size']['name']
                item['company_nature'] = each_group['company']['type']['name']

                item['crawl_date'] = self.date_time
                item['crawl_url'] = response.request.url

                yield item

            url = "https://fe-api.zhaopin.com/c/i/sou?start={0}&pageSize=90&cityId={1}&workExperience=-1&education=-1&companyType=-1&employmentType=-1&jobWelfareTag=-1&kw={2}&kt=3&_v=0.34053159&x-zp-page-request-id=d1dd66ee655347939b69acf87870ccc1-1548983897497-953047".format(self.page_count * 90,self.city_list[self.city_count],self.query_list[self.query_count])
            yield scrapy.Request(url=url, callback=self.parse)
        else:
            if self.query_count < len(self.query_list) - 1:
                self.query_count += 1

            elif self.city_count < len(self.city_list) -1:
                self.query_count = 0
                self.city_count += 1

            else:
                return

            url = "https://fe-api.zhaopin.com/c/i/sou?start={0}&pageSize=90&cityId={1}&workExperience=-1&education=-1&companyType=-1&employmentType=-1&jobWelfareTag=-1&kw={2}&kt=3&_v=0.34053159&x-zp-page-request-id=d1dd66ee655347939b69acf87870ccc1-1548983897497-953047".format(0,self.city_list[self.city_count],self.query_list[self.query_count])
            self.page_count = 0
            yield scrapy.Request(url=url, callback=self.parse)

                
            

