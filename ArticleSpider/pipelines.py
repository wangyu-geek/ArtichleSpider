# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scra-py.org/en/latest/topics/item-pipeline.html
import codecs
import json
import MySQLdb
import MySQLdb.cursors
from scrapy.pipelines.images import ImagesPipeline
from scrapy.exporters import JsonItemExporter
from twisted.enterprise import adbapi


class ArticlespiderPipeline(object):
    def process_item(self, item, spider):
        return item

class JsonWithEncodingPipline(object):
    # 自定义 json 文件的导出
    def __init__(self):
        # 初始化时，打开 json 文件
        self.file = codecs.open("article.json", "w", encoding="utf-8")

    def process_item(self, item, spider):
        # 先将 item 转成字典，然后转成 json
        lines = json.dumps(dict(item), ensure_ascii=False) + "\n"
        self.file.write(lines)
        # 注意此处将item 返回，以备接下来的函数调用
        return item

    def spider_closed(self, spider):
        # 关闭文件
        self.file.close()

class JsonExporterPipeline(object):
    # 调用 scrapy 提供的 json export 导出 json 文件
    def __init__(self):
        # 打开方式 b 是二进制
        self.file = open("articleexporter.json", "wb")
        self.exporter = JsonItemExporter(self.file, encoding = "utf-8", ensure_ascii = False)
        # 开始导出
        self.exporter.start_exporting()

    def close_spider(self, spider):
        # 停止导出
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item = item)
        return item


class ArticleImagePipeline(ImagesPipeline):
    def item_completed(self, results, item, info):
        if "front_image_url" in item:
            for ok, value in results:
                image_path = value['path']
            item["front_image_path"] = image_path
        return item


class MysqlPipeline(object):
    def __init__(self):
        self.conn = MySQLdb.connect("127.0.0.1", "wangyu", "wangyu", "article_spider", charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        insert_sql = """
            INSERT INTO jobbole_article(title, url, url_object_id, front_image_url, front_image_path, comment_nums, 
            fav_nums, praise_nums, tags, content)
              VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s,)
        """
        self.cursor.execute(insert_sql, (item["title"], item["url"], item["url_object_id"], item["front_image_url"],
                                         item["front_image_path"], item["comment_nums"], item["fav_nums"],
                                         item["praise_nums"], item["tags"], item["content"]))
        self.conn.commit()
        return item


class MysqlTwistedPipline(object):
    def __init__(self, dbpool):
        # 在 spider 运行时，就把 dbpool 传递进来了
        # 获得数据库连接池
        self.dbpool = dbpool

    @classmethod
    def from_settings(cls, settings):
        # 读取配置文件
        dbparams = dict(
            host=settings["MYSQL_HOST"],
            db=settings["MYSQL_DB"],
            user=settings["MYSQL_USER"],
            passwd=settings["MYSQL_PWD"],
            charset='utf8',
            cursorclass=MySQLdb.cursors.DictCursor,
            use_unicode=True
        )
        # adb 可以把 mysql 变成异步操作
        dbpool = adbapi.ConnectionPool("MySQLdb", **dbparams)
        return cls(dbpool)

    def process_item(self, item, spider):
        # 使用 twisted 将 mysql 插入变成异步执行
        query = self.dbpool.runInteraction(self.do_insert, item)
        # 处理异常
        query.addErrback(self.handle_error, item, spider)

    def handle_error(self, failure, item, spider):
        # 处理异步插入的异常
        print(failure)

    def do_insert(self, cursor, item):
        # 执行具体的插入，自动提交
        insert_sql, params = item.get_insert_sql()
        cursor.execute(insert_sql, params)

class ElasticsearchPipeline(object):
    # 将数据写入到 es 中
    def process_item(self, item, spider):
        # 将 item 转为 es 的数据
        item.save_to_es()
        return item


























