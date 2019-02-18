# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.conf import settings
import pymongo
import pymysql
import pandas as pd
import sqlalchemy


class JobsPipelineMongodb(object):
    def __init__(self):
        host = settings['MONGODB_HOST']
        port = settings['MONGODB_PORT']
        dbName = settings['MONGODB_DBNAME']
        client = pymongo.MongoClient(host=host, port=port, connect=False)
        tdb = client[dbName]
        self.post = tdb[settings['MONGODB_DOCNAME']]

    def process_item(self, item, spider):
        job_info = dict(item)
        self.post.insert(job_info)
        return item


class JobsPipelineMySQL(object):
    def __init__(self):

        self.mydb = sqlalchemy.create_engine("mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8".format(
            settings['MYSQL_USER'], settings['MYSQL_PASSWORD'], settings['MYSQL_HOST'], settings['MYSQL_PORT'], settings['MYSQL_DATABASE']))

    def process_item(self, item, spider):
        job_info_dict = dict(item)
        job_info_crawl_dict = {}

        if 'crawl_date' in job_info_dict and 'crawl_url' in job_info_dict and 'crawl_pages' in job_info_dict:
            job_info_crawl_dict['crawl_date'] = job_info_dict['crawl_date']
            job_info_crawl_dict['crawl_url'] = job_info_dict['crawl_url']
            job_info_crawl_dict['crawl_pages'] = job_info_dict['crawl_pages']

            del job_info_dict['crawl_date']
            del job_info_dict['crawl_url']
            del job_info_dict['crawl_pages']

        job_info_df = pd.DataFrame([job_info_dict])
        job_info_crawl_df = pd.DataFrame([job_info_crawl_dict])
        try:
            job_info_df.to_sql(spider.name,self.mydb,settings['MYSQL_DATABASE'],if_exists='append',index=False)
        except sqlalchemy.exc.IntegrityError:                  #主键（url）已存在
            print("Already exist in database!")

        try:
            job_info_crawl_df.to_sql(spider.name + settings['MYSQL_SUFFIX_CRAWLED'],self.mydb,settings['MYSQL_DATABASE'],if_exists='append',index=False)
        except sqlalchemy.exc.IntegrityError:                  #主键（url）已存在
            print("Already exist in database!")
        
        return item
