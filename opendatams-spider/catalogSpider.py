from crawler.spider import Spider
from crawler.writter import DataWriter
from lxml import html
from JsonParse import JsonParse
import traceback
from crawler.logger import Logger
import json


data = []
headers = {'cata_id': 'id',
           'cata_title': '数据目录名称',
           'data_count': '数据量',
           'file_count': '文件数',
           'api_count': '接口数量',
           'open_type': '开放状态',
           'topic_name': '所属主题',
           'update_time': '最后更新',
           'org_name': '来源部门',
           'cata_tags': '标签',
           'conf_update_cycle': '更新频率',
           'conf_use_type': '数据格式',
           'released_time': '发布时间',
           'group_name': '所属行业',
           'description': '简介',
           'use_file_count': '下载次数',
           'use_visit': '浏览次数',
           'use_grade': '评分人数',
           'use_task_count': '评价次数',
           'use_points': '评分总数',
           'use_scores': '平均评分',
           'data_count': '数据条目',
           'download_url': '下载URL http://www.fsdata.gov.cn/data/catalog/catalogDetail.do?method=getFileDownloadAddr&fileId=',
           'download_file': '文件名称',
           'download_size': '文件大小', }

logger = Logger(__name__)


def getdata(response):
    try:
        content = json.loads(response['text'])
        parse = JsonParse()
        parse.parse(content)
        data.append(parse.getItems())
        headers.update(parse.getHeaders())
        logger.warn(data)
    except Exception as e:
        traceback.print_exc()

        # 信息解析不太对


if __name__ == '__main__':
    page = 1
    requests = []
    for start in range(page):
        start *= 6
        url = "http://www.fsdata.gov.cn/data/catalog/catalog.do?method=GetCatalog&data={&_order=cc.update_time desc&org_code&group_id&use_type&catalog_format&keywords&tag&grade&cata_type=default&start="+str(
            start)+"&length=6&pageLength=6&}"
        requests.append({'url': url})

    spider = Spider(requests,
                    getUrl_func=None, getData_func=getdata, level=1)
    spider.start_multiThread_crawl(1)

    writter = DataWriter('catalog.csv', headers=headers)
    writter.write(data)
