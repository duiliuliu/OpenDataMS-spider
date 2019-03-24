from crawler.spider import Spider
from crawler.writter import DataWriter
from lxml import html
from JsonParse import JsonParse
import traceback
import json
from crawler.logger import Logger

logger = Logger(__name__)

data = []


def getdata(response):
    try:
        
        content = json.loads(response['text'])
        content = content['data']
        logger.warn(content)
        data.extend(content)
    except Exception as e:
        traceback.print_exc()


if __name__ == '__main__':
    param = {
        'cata_id': '95222',
        'conf_type': 2
    }
    request1 = {
        'url': 'http://www.fsdata.gov.cn/data/catalog/catalogDetail.do?method=GetDownLoadInfo',
        'request_type': 'post',
        'data': param
    }

    request2 = {
        'url': 'http://www.fsdata.gov.cn/data/catalog/catalogDetail.do?method=GetDownLoadInfo',
        'request_type': 'post',
        'data': {
        'cata_id': '39697',
        'conf_type': 2
    }
    }



    spider1 = Spider(request1,
                    getUrl_func=None, getData_func=getdata, level=1)
    spider1.start_crawl()

    writter = DataWriter('download_info_list.csv')
    writter.write(data)

    spider2 = Spider(request2,
                    getUrl_func=None, getData_func=getdata, level=1)
    spider2.start_crawl()


    writter = DataWriter('download_info_list2.csv')
    writter.write(data)

    
