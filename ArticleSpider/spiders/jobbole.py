# -*- coding: utf-8 -*-
import scrapy
import re
import datetime
from scrapy.http import Request
from urllib import parse
from ArticleSpider.items import JobBoleArticleItem
from ArticleSpider.utils.common import get_md5
from ArticleSpider.items import ArticleItemLoader

class JobboleSpider(scrapy.Spider):
    name = 'jobbole'
    allowed_domains = ['blog.jobbole.com']
    start_urls = ['http://blog.jobbole.com/all-posts/']

    def parse(self, response):
        """
        1. 获取文章列表页中的文章 url 并交给 scrapy 下载后并进行解析
        2. 获取下一页的 url 并交给 scrapy 进行下载，下载完成后交给 pase
        """
        # 获取 文章列表的文章 url，并交给 scrapy  下载
        post_nodes = response.css("#archive div.floated-thumb .post-thumb a")
        for post_node in post_nodes:
            # 调用 request 下载 ，parse_detail 是回调函数
            # urljoin 设置文章的 url 为绝对路径
            # 获取图片和文章的 url
            image_url = post_node.css("img::attr(src)").extract_first("")
            post_url = post_node.css("::attr(href)").extract_first("")
            yield Request(url=parse.urljoin(response.url, post_url), meta={"front_image_url":image_url}, callback=self.parse_detail)
            # 提取下一页，并交给 scrapy 下载
        next_url = response.css("a.next.page-numbers::attr(href)").extract_first()
        yield Request(url=next_url, callback=self.parse)


    def parse_detail(self, response):
        # scrapy 自动 pipelines 的 process_item
        front_image_url = response.meta.get("front_image_url", "")
        item_loader = ArticleItemLoader(item=JobBoleArticleItem(), response=response)
        item_loader.add_css("title", ".entry-header h1::text")
        item_loader.add_value("url", response.url)
        item_loader.add_value("url_object_id", get_md5(response.url))
        item_loader.add_value("create_date", ".entry-meta-hide-on-mobile::text")
        item_loader.add_value("front_image_url", [front_image_url])
        item_loader.add_css("praise_nums", ".vote-post-up h10::text")
        item_loader.add_css("comment_nums", "a[href='#article-comment'] span::text")
        item_loader.add_css("fav_nums", ".bookmark-btn::text")
        item_loader.add_css("tags", "p.entry-meta-hide-on-mobile a::text")
        item_loader.add_css("content", ".entry")
        article_item = item_loader.load_item()
        yield article_item