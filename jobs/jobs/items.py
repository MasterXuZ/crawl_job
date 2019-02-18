# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class JobsItem(scrapy.Item):
    # define the fields for your item here like:
    job_title = scrapy.Field()
    salary = scrapy.Field()
    experience = scrapy.Field()
    location = scrapy.Field()
    key_word = scrapy.Field()
    detail_url = scrapy.Field()
    update_date = scrapy.Field()
    welfare = scrapy.Field()
    company_title = scrapy.Field()
    company_scale = scrapy.Field()
    company_nature = scrapy.Field()

    crawl_date = scrapy.Field()
    crawl_url = scrapy.Field()
    crawl_pages = scrapy.Field()


