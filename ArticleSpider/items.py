# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy
import datetime
import re
from scrapy.loader.processors import MapCompose, TakeFirst, Join
from scrapy.loader import ItemLoader
from ArticleSpider.utils.common import get_nums
from ArticleSpider.settings import SQL_DATE_FORMAT, SQL_DATETIME_FORMAT
from w3lib.html import remove_tags
import time

class ArticlespiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


def add_jobbole(value):
    return value + "-jobbole"


def date_convert(value):
    try:
        create_date = datetime.datetime.strptime(value, "%Y%m%d").date()
    except Exception as e:
        create_date = datetime.datetime.now().date()
    return create_date


def remove_comment_tags(value):
    # 去掉tag中提取的评论
    if "评论" in value:
        return ""
    else:
        return value


def return_value(value):
    return value


class ArticleItemLoader(ItemLoader):
    default_output_processor = TakeFirst()


class JobBoleArticleItem(scrapy.Item):
    title = scrapy.Field(
        input_processor=MapCompose(add_jobbole),
    )
    create_date = scrapy.Field(
        input_processor=MapCompose(date_convert),
    )
    url = scrapy.Field()
    url_object_id = scrapy.Field()
    front_image_url = scrapy.Field(
        output_processor=MapCompose(return_value)
    )
    front_image_path = scrapy.Field()
    praise_nums = scrapy.Field(
        input_processor=MapCompose(get_nums)
    )
    comment_nums = scrapy.Field(
        input_processor=MapCompose(get_nums)
    )
    fav_nums = scrapy.Field(
        input_processor=MapCompose(get_nums)
    )
    tags = scrapy.Field(
        input_processor=MapCompose(remove_comment_tags),
        output_processor=Join(",")
    )
    content = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """
          INSERT INTO jobbole_article(title, create_date, url, url_object_id, front_image_url, front_image_path, comment_nums,
          fav_nums, praise_nums, tags, content)
          VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (self['title'], self["create_date"], self["url"], self["url_object_id"], self["front_image_url"],
                  self["front_image_path"], self["comment_nums"], self["fav_nums"], self["praise_nums"],
                  self["tags"], self["content"])
        return insert_sql, params


class ZhihuQuestionItem(scrapy.Item):
    # 知乎的问题 item
    zhihu_id = scrapy.Field()
    topics = scrapy.Field()
    url = scrapy.Field()
    title = scrapy.Field()
    content = scrapy.Field()
    answer_num = scrapy.Field()
    comments_num = scrapy.Field()
    watch_user_num = scrapy.Field()
    click_num = scrapy.Field()
    create_time = scrapy.Field()
    update_time = scrapy.Field()
    crawl_time = scrapy.Field()
    crawl_update_time = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """
                  INSERT INTO zhihu_question(zhihu_id, topics, url, title, content,
                   answer_num, comments_num, watch_user_num, click_num, crawl_time)
                  VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                  ON DUPLICATE KEY UPDATE content=VALUES(content), comments_num=VALUES(comments_num),
                  watch_user_num=VALUES (watch_user_num), click_num=VALUES (click_num), 
                   answer_num=VALUES (answer_num)
                """

        zhihu_id = "".join(self['zhihu_id'])
        topics = "".join(self['topics'])
        url = self['url'][0]
        title = "".join(self['title'])
        content = "".join(self['content'])
        answer_num = get_nums("".join(self['answer_num']))
        comments_num = get_nums("".join(self['comments_num']))
        watch_user_num = get_nums("".join(self['watch_user_num'][0]))
        click_num = get_nums("".join(self['watch_user_num'][1]))
        crawl_time = datetime.datetime.now().strftime(SQL_DATETIME_FORMAT)
        params = (zhihu_id, topics, url, title, content, answer_num, comments_num, watch_user_num, click_num, crawl_time)
        return insert_sql, params


class ZhihuAnswerItem(scrapy.Item):
    # 知乎的回答 item
    zhihu_id = scrapy.Field()
    url = scrapy.Field()
    question_id = scrapy.Field()
    author_id = scrapy.Field()
    content = scrapy.Field()
    praise_num = scrapy.Field()
    comments_num = scrapy.Field()
    crawl_time = scrapy.Field()
    crawl_update_time = scrapy.Field()
    create_time = scrapy.Field()
    update_time = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """
                  INSERT INTO zhihu_answer(zhihu_id, url, question_id, author_id, content, praise_num,
                  comments_num, crawl_time, create_time, update_time)
                  VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                  ON DUPLICATE KEY UPDATE content=VALUES(content), comments_num=VALUES(comments_num),
                  update_time=VALUES(update_time), praise_num=VALUES(praise_num)
                """

        create_time = datetime.datetime.fromtimestamp(self['create_time']).strftime(SQL_DATETIME_FORMAT)
        update_time = datetime.datetime.fromtimestamp(self['update_time']).strftime(SQL_DATETIME_FORMAT)
        crawl_time = datetime.datetime.now().strftime(SQL_DATETIME_FORMAT)

        params = (self['zhihu_id'], self['url'], self['question_id'], self['author_id'], self['content'],
                  self['praise_num'], self['comments_num'], crawl_time, create_time, update_time)
        return insert_sql, params


def remove_splash(value):
    # 去掉城市中的斜线
    return value.replace("/", "")


def handle_jobaddr(value):
    addr_list = value.split("\n")
    addr_list = [item.strip() for item in addr_list if item.strip() != "查看地图"]
    return "".join(addr_list)


class LagouJobItemLoader(ItemLoader):
    default_output_processor = TakeFirst()


class LagouJobItem(scrapy.Item):
    url = scrapy.Field()
    url_object_id = scrapy.Field()
    title = scrapy.Field()
    salary = scrapy.Field()
    salary_min = scrapy.Field()
    salary_max = scrapy.Field()
    work_years_min = scrapy.Field()
    work_years_max = scrapy.Field()
    job_type = scrapy.Field()
    publish_time = scrapy.Field()
    tags = scrapy.Field(
        input_processor=Join(",")
    )
    job_advantage = scrapy.Field()
    job_desc = scrapy.Field()
    job_addr = scrapy.Field(
        input_processor=MapCompose(remove_tags, handle_jobaddr)
    )
    company_url = scrapy.Field()
    company_name = scrapy.Field()
    crawl_time = scrapy.Field()
    crawl_update_time = scrapy.Field()
    job_city = scrapy.Field(
        input_processor=MapCompose(remove_splash)
    )
    work_years = scrapy.Field(
        input_processor=MapCompose(remove_splash)
    )
    degree_need = scrapy.Field(
        input_processor=MapCompose(remove_splash)
    )

    def get_insert_sql(self):
        insert_sql = """
            insert into lagou_job(title, url, url_object_id, salary, work_years, degree_need,
            job_type, publish_time, tags, job_advantage, job_desc, job_addr, company_url, company_name,
            job_city, crawl_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE salary=VALUES(salary), job_desc=VALUES(job_desc)
        """
        params = (
            self["title"], self["url"], self["url_object_id"], self["salary"], self["work_years"], self["degree_need"],
            self["job_type"], self["publish_time"], self["tags"], self["job_advantage"], self["job_desc"], self["job_addr"],
            self["company_url"], self["company_name"], self["job_city"], self["crawl_time"].strftime(SQL_DATETIME_FORMAT),
        )
        return insert_sql, params















