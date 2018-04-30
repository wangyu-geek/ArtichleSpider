from datetime import datetime
from elasticsearch_dsl import DocType, Date, Nested, Boolean, \
    analyzer, Completion, Keyword, Text, Integer
from elasticsearch_dsl.analysis import CustomAnalyzer as _CustomAnalyzer
from elasticsearch_dsl.connections import connections
connections.create_connection(hosts=["localhost"])

class CustomAnalyzer(_CustomAnalyzer):
    def get_analysis_definition(self):
        return {}

ik_analyzer = CustomAnalyzer("ik_max_word", filter=["lowercase"])

class ArticleType(DocType):
    # 伯乐在线文章类型
    suggest = Completion(analyzer=ik_analyzer)
    title = Text(analyzer="ik_max_word")
    create_date = Date()
    url = Keyword()
    url_object_id = Keyword()
    front_image_url = Keyword()
    front_image_path = Keyword()
    praise_nums = Integer()
    comment_nums = Integer()
    fav_nums = Integer()
    tags = Text(analyzer="ik_max_word")
    content = Text(analyzer="ik_max_word")

    class Meta:
        index = "jobbole"
        doc_type = "article"


class LagouType(DocType):
    url = Keyword()
    url_object_id = Keyword()
    title = Text(analyzer="ik_max_word")
    salary = Keyword()
    work_years = Keyword()
    degree_need = Keyword()
    job_type = Keyword()
    publish_time = Keyword()
    tags = Text(analyzer='ik_max_word')
    job_advantage = Text(analyzer="ik_max_word")
    job_desc = Text(analyzer="ik_max_word")
    job_addr = Text(analyzer="ik_max_word")
    company_url = Keyword()
    company_name = Text(analyzer="ik_max_word")
    job_city = Keyword()

    class Meta:
        index = "zhaopin"
        doc_type = "lagou"


class JobType(DocType):
    suggest = Completion(analyzer=ik_analyzer)
    url = Keyword()
    url_object_id = Keyword()
    title = Text(analyzer="ik_max_word")
    city = Keyword()
    work_years = Keyword()
    degree_need = Keyword()
    job_type = Keyword()
    publish_time = Keyword()
    tags = Text(analyzer='ik_max_word')
    job_advantage = Text(analyzer="ik_max_word")
    job_desc = Text(analyzer="ik_max_word")
    addr = Text(analyzer="ik_max_word")
    company_url = Keyword()
    company_name = Text(analyzer="ik_max_word")
    website = Keyword()
    salary_min = Integer()
    salary_max = Integer()
    release_time = Date()

    class Meta:
        index = "zhaopin"
        doc_type = "job"

if __name__ == "__main__":
    # ArticleType.init()
    # LagouType.init()
    JobType.init()