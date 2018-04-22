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
from models.es_type import ArticleType, LagouType
from elasticsearch_dsl.connections import connections
es_article = connections.create_connection(ArticleType._doc_type.using)
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

def gen_suggests(index, info_tuple):
    # [{"input":[], "weight":2}]
    # 根据字符串生成搜索建议数据
    used_words = set()
    suggests = []
    for text, weight in info_tuple:
        if text:
            # 调用 es 的 analyzer 接口分析
            words = es_article.indices.analyze(index=index, analyzer="ik_max_word",
                                       params={'filter':['lowercase']}, body=text)
            analyzed_words = set(r["token"] for r in words["tokens"] if len(r['token']) > 1)
            new_words = analyzed_words - used_words
        else:
            new_words = set()
        if new_words:
            suggests.append({"input":list(new_words), "weight":weight})
    return suggests

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

    def save_to_es(self):
        article = ArticleType()
        article.title = self['title']
        article.create_date = self['create_date']
        article.content = remove_tags(self['content'])
        article.front_image_url = self['front_image_url']
        if 'front_image_path' in self:
            article.front_image_path = self['front_image_path']
        article.praise_nums = self['praise_nums']
        article.fav_nums = self['fav_nums']
        article.comment_nums = self['comment_nums']
        article.url = self['url']
        article.tags = self['tags']
        article.meta.id = self['url_object_id']  # es 中的id
        # 生成搜索建议
        article.suggest = gen_suggests(ArticleType._doc_type.index, ((article.title,10), (article.tags, 7)))
        article.save()
        return article

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


def remove_div(value):
    # 去掉 div
    return remove_tags(value, which_ones=("div",))


def handle_yingcai_addr(value):
    addr = value.split(" ")
    return addr[0]


def get_min_salary(value):
    value = re.findall("(\d+)", value)
    length = len(value)
    if length == 2:
        value = int(value[0])
        return value
    elif length == 1:
        value = int(value[0])
        return value
    else:
        return 0


def get_max_salary(value):
    value = re.findall("(\d+)", value)
    length = len(value)
    if length == 2:
        value = int(value[1])
        return value
    elif length == 1:
        return 9999999
    else:
        return 0


class BaseJobItemLoader(ItemLoader):
    # 职位 item loader 基类
    default_output_processor = TakeFirst()

class  BaseJobItem(scrapy.Item):
    # 职位信息基类
    url = scrapy.Field()
    url_object_id = scrapy.Field()
    title = scrapy.Field()
    salary = scrapy.Field()
    city = scrapy.Field()
    work_years = scrapy.Field()
    degree_need = scrapy.Field()
    job_type = scrapy.Field()
    publish_time = scrapy.Field()
    tags = scrapy.Field(
        output_processor=Join(",")
    )
    job_advantage = scrapy.Field()
    job_desc = scrapy.Field()
    addr = scrapy.Field()
    company_url = scrapy.Field()
    company_name = scrapy.Field()
    crawl_time = scrapy.Field()
    crawl_update_time = scrapy.Field()
    website = scrapy.Field()
    salary_min = scrapy.Field()
    salary_max = scrapy.Field()
    release_time = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """
            insert into job(title, url, url_object_id, salary, work_years, degree_need,
            job_type, publish_time, tags, job_advantage, job_desc, addr, company_url, company_name,
            city, crawl_time, website)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE salary=VALUES(salary), job_desc=VALUES(job_desc)
        """
        params = (
            self["title"], self["url"], self["url_object_id"], self["salary"], self["work_years"], self["degree_need"],
            self["job_type"], self["publish_time"], self["tags"], self["job_advantage"], self["job_desc"], self["addr"],
            self["company_url"], self["company_name"], self["city"], self["crawl_time"].strftime(SQL_DATETIME_FORMAT),
            self["website"]
        )
        return insert_sql, params

def handle_jobaddr(value):
    addr_list = value.split("\n")
    addr_list = [item.strip() for item in addr_list if item.strip() != "查看地图"]
    return "".join(addr_list)

def get_lagou_salary(value):
    return value*1000


def get_lagou_release_time(value):
    if "1天前" in value:
        return getYesterday()
    if "2天前" in value:
        return getBeforeYesterday()
    if "3天前" in value:
        return getThreeDaysAgo()
    pattern = "([\d]{1,2}:[\d]{1,2})"
    if re.match(pattern=pattern, string=value):
        return datetime.datetime.now().strftime(SQL_DATE_FORMAT)
    pattern = "([\d]{4}\-[\d]{1,2}\-[\d]{1,2})"
    result = re.match(pattern=pattern, string=value)
    if result:
        return result.group(0)
    return datetime.datetime.now().strftime(SQL_DATE_FORMAT)

class LagouJobItemLoader(ItemLoader):
    default_output_processor = TakeFirst()


class LagouJobItem(BaseJobItem):
    tags = scrapy.Field(
        output_processor=Join(",")
    )
    addr = scrapy.Field(
        input_processor=MapCompose(remove_tags, handle_jobaddr)
    )
    city = scrapy.Field(
        input_processor=MapCompose(remove_splash),
    )
    work_years = scrapy.Field(
        input_processor=MapCompose(remove_splash)
    )
    degree_need = scrapy.Field(
        input_processor=MapCompose(remove_splash)
    )
    website = scrapy.Field()
    salary_min = scrapy.Field(
        input_processor=MapCompose(get_min_salary, get_lagou_salary),
        output_processor=MapCompose(return_value)
    )
    salary_max = scrapy.Field(
        input_processor=MapCompose(get_max_salary, get_lagou_salary),
        output_processor=MapCompose(return_value)
    )
    release_time = scrapy.Field(
        input_processor=MapCompose(get_lagou_release_time),
        output_processor=MapCompose(return_value)
    )

    def exists_value(self):
        try:
            if self["tags"] is None:
                self["tags"] = ""
                print('tags not exists')
        except BaseException:
            print('tags exception')
            self["tags"] = ""

    def get_insert_sql(self):
        insert_sql = """
            insert into job(title, url, url_object_id, salary, work_years, degree_need,
            job_type, publish_time, tags, job_desc, addr, company_url, company_name,
            city, crawl_time, website, salary_min, salary_max, release_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE salary=VALUES(salary), job_desc=VALUES(job_desc)
        """
        params = (
            self["title"], self["url"], self["url_object_id"], self["salary"], self["work_years"], self["degree_need"],
            self["job_type"], self["publish_time"], self["tags"], self["job_desc"], self["addr"],
            self["company_url"], self["company_name"], self["city"], self["crawl_time"].strftime(SQL_DATETIME_FORMAT),
            self["website"], self["salary_min"], self["salary_max"], self["release_time"]
        )
        return insert_sql, params

    def save_to_es(self):
        lagou = LagouType()
        lagou.url = self['url']
        lagou.url_object_id = self['url_object_id']
        lagou.title = self['title']
        lagou.salary = self['salary']
        lagou.work_years = self['salary']
        lagou.degree_need = self['degree_need']
        lagou.job_type = self['job_type']
        lagou.publish_time = self['publish_time']
        lagou.tags = self['tags']
        lagou.job_advantage = self['job_advantage']
        lagou.job_desc = self['job_desc']
        lagou.job_addr = self['job_addr']
        lagou.company_url = self['company_url']
        lagou.company_name = self['company_name']
        lagou.job_city = self['job_city']

        lagou.save()
        return lagou


def yingcai_salary_min(value):
    # 获取英才网最低薪资
    if "面议" in value:
        return 0
    else:
        return get_min_salary(value)


def yingcai_salary_max(value):
    # 获取英才网最低薪资
    if "面议" in value:
        return 0
    else:
        return get_max_salary(value)

def getThreeDaysAgo():
    return getPreDay(2)

def getBeforeYesterday():
    return getPreDay(2)

def getYesterday():
    return getPreDay(1)

def getPreDay(days=1):
    today = datetime.date.today()
    days = datetime.timedelta(days)
    yesterday = today - days
    return yesterday

def get_yingcai_release_time(value):
    # 获取英才网职位发布时间
    if "今天" in value:
        return datetime.datetime.now().strftime(SQL_DATE_FORMAT)
    if "昨天" in value:
        return getYesterday()
    result = re.match("((\d){1,2}\-(\d){1,2})", value)
    if result:
        release = str(datetime.datetime.now().year)+"-"+result.group(0)
        return release
    result = re.match("([\d]{4}\-[\d]{1,2}\-[\d]{1,2})", value)
    if result:
        return result.group(0)
    return datetime.datetime.now().strftime(SQL_DATE_FORMAT)

class YingCaiJobItemLoader(BaseJobItemLoader):
    # 英才网 item loader 基类
    default_output_processor = TakeFirst()


class YingCaiJobItem(BaseJobItem):
    # 中华英才网 item
    job_desc = scrapy.Field(
        input_processor=MapCompose(remove_div)
    )
    city = scrapy.Field(
        input_processor=MapCompose(handle_yingcai_addr),
        output_processor=MapCompose(return_value)
    )
    salary_min = scrapy.Field(
        input_processor=MapCompose(yingcai_salary_min),
        output_processor=MapCompose(return_value)
    )
    salary_max = scrapy.Field(
        input_processor=MapCompose(yingcai_salary_max),
        output_processor=MapCompose(return_value)
    )
    release_time = scrapy.Field(
        input_processor=MapCompose(get_yingcai_release_time),
        output_processor=MapCompose(return_value)
    )

    def get_insert_sql(self):
        insert_sql = """
            insert into job(title, url, url_object_id, salary, work_years, degree_need,
            job_type, publish_time, tags, job_desc, addr, company_url, company_name,
            city, crawl_time, website, salary_min, salary_max, release_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE salary=VALUES(salary), job_desc=VALUES(job_desc)
        """
        params = (
            self["title"], self["url"], self["url_object_id"], self["salary"], self["work_years"], self["degree_need"],
            self["job_type"], self["publish_time"], self["tags"], self["job_desc"], self["addr"],
            self["company_url"], self["company_name"], self["city"], self["crawl_time"].strftime(SQL_DATETIME_FORMAT),
            self["website"], self["salary_min"], self["salary_max"], self["release_time"]
        )
        return insert_sql, params
