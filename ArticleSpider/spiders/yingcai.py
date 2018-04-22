# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from items import YingCaiJobItemLoader, YingCaiJobItem
from ArticleSpider.utils.common import get_md5
from datetime import datetime


class YingcaiSpider(CrawlSpider):
    name = 'yingcai'
    allowed_domains = ['www.chinahr.com']
    start_urls = ['http://www.chinahr.com/job/6618028702960644.html']

    rules = (
        Rule(LinkExtractor(allow=("beijing/jobs/*")), follow=True),
        # Rule(LinkExtractor(allow=("beijing/*")), follow=True),
        # Rule(LinkExtractor(allow=("sou/*")), follow=True),
        Rule(LinkExtractor(allow=r'job/\d+.html'), callback='parse_job', follow=True),
    )

    def parse_job(self, response):
        # 解析英才网职位
        item_loader = YingCaiJobItemLoader(item=YingCaiJobItem(), response=response)
        item_loader.add_value("url", response.url)
        item_loader.add_value("url_object_id", get_md5(response.url))
        item_loader.add_css("title", ".job_name::text")
        item_loader.add_css("salary", ".job_price::text")
        item_loader.add_css("salary_min", ".job_price::text")
        item_loader.add_css("salary_max", ".job_price::text")
        item_loader.add_css("city", ".job_require .job_loc::text")
        item_loader.add_css("work_years", ".job_exp::text")
        item_loader.add_xpath("degree_need", "//*[@class='job_require']/span[4]/text()")
        item_loader.add_xpath("job_type", "//*[@class='job_require']/span[3]/text()")
        item_loader.add_css("publish_time", ".updatetime::text")
        item_loader.add_css("release_time", ".updatetime::text")
        item_loader.add_css("tags", ".job_fit_tags ul.clear li::text")
        item_loader.add_css("job_desc", ".job_intro_info")
        item_loader.add_css("addr", ".job_require .job_loc::text")
        item_loader.add_css("company_url", ".job-company h4 a::attr(href)")
        item_loader.add_css("company_name", ".job-company h4 a::text")
        item_loader.add_value("crawl_time", datetime.now())
        item_loader.add_value("website", "中华英才网")
        job_item = item_loader.load_item()

        return job_item
