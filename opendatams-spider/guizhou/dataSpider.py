from sspider import Spider, Request, Response, HtmlParser, XlsxWritter, JsonWritter
import requests

import sys
sys.path.append('..')
from util import saveContent

# 如何运行该爬虫：
# 下载安装python环境
# 下载安装爬虫库 sspider : 使用该命令即可`pip install sspider`

# 贵州开放数据 url:http://data.guizhou.gov.cn/dataopen/api/dataset
# dataType:   全部
# dataType: 0 文件
# dataType: 1 api
# dataType: 3 应用

# url
url = 'http://data.guizhou.gov.cn/dataopen/api/dataset'
file_key = '贵州文件'

# 请求沉睡时间，防止一瞬间过多的请求被目的网站封禁，单位：秒
sleep_time = 0

# 构建请求参数


def getParams(pageNo,):
    return {
        # 身份id
        'callback': 'jQuery111303623122905798215_1555829592556',
        'pageNo': pageNo,  # 页 ，{}表示未知数，之后进行填充
        'pageSize': '10',  # 每页数据数量
        'order': '0',
        'topicId': '',
        'orgId': '',
        'name': '',
        'scoreLow': '',
        'scoreHigh': '',
        'dataType': '0',
        '_': '1555811371572'
    }


# range(1) 表示抓取1页，数据集共有143页，如需拆去全部数据，请改为143
reqs = [Request(
    'get', url, params=getParams(i)) for i in range(1, 2)]


# 获取数据集目录的解析器
class CatalogParser(HtmlParser):
    def parse(self, response):
        data = response.jsonp()['data']['datasetlist']
        # 需要额数据
        items = []

        for item in data:
            items.append({
                '数据摘要': item['description'],
                '浏览量': item['views'],
                '最后更新时间': item['updTime'],
                '主题名称': item['topicName'],
                '数据提供方': item['orgName'],
                '数据名称': item['name'],
                # '更新时间': item['updTime'],
                'id': item['id'],
                'list': item['list']
            })

        # 返回继续抓取的请求与解析到的数据，此处无继续抓取请求
        return [], items


# 数据写入类，此处使用xlsx格式数据写入
writter = XlsxWritter(writeMode=XlsxWritter.WritterMode.EXTEND)


# 建立爬虫对象，解析类是CatalogParser
cataSpider = Spider(parser=CatalogParser(), writter=writter)
cataSpider.run(reqs)
# 数据保存
cataSpider.write(file_key+'catalog.xlsx', write_header=True)

############################################################################

# 根据目录信息继续抓取api接口信息并进行文件下载
# 获取目录信息
cata = cataSpider.getItems(type='dict')

url_template = 'http://data.guizhou.gov.cn/dataopen/api/filedata/{}?callback=jQuery111308858001035427423_1556328325340&_=1556328325344'

# 根据目录构建请求,method='get',url,other_info为request可携带存储的与网络请求无关的信息，此处存储数据集名称,sleep_time表示睡眠一段时间后进行请求
dimenRequests = []
for cata_item in cata:
    for i in cata_item['list']:
        dimenRequests.append(
            Request('get', url_template.format(i['id']), other_info=cata_item)
        )


class DimensionParser(HtmlParser):
    def parse(self, response):
        data = response.jsonp()['data']
        #  获取请求中保存的信息
        item = response.request.other_info

        if 'file' not in item:
            item['file'] = []
        try:
            item['file'].append({
                'id': data['id'],
                'format': data['format'],
                'name': data['name'],
                'remark': data['remark'],
                'shortUrl': data['shortUrl'],
                'updateTime': data['updTime']
            })
        except:
            item['file'].append(data['id']+'异常')

        # 文件下载
        download_url = 'http://gzopen.oss-cn-guizhou-a.aliyuncs.com/{}?OSSAccessKeyId=cRMkEl0MLhpV9l7g&Signature=nGRFRbzguzVp5QA3BnR9cx38LRk%3D'
        res = requests.get(download_url.format(data['remark']))
        dir = item['数据名称']+'/' if item['数据名称'] else '未知目录/'
        path = 'source/'+dir+data['remark']
        saveContent(path, res.content)

        return [], item


# 将数据写进xlsx中，因为数据有多层嵌套，所以此时选择写入xlsx文件中，如需要写入json中，取消下方注释即可
dimenInfoWritter = XlsxWritter(writeMode=XlsxWritter.WritterMode.APPEND)
# dimenInfoWritter = JsonWritter(writeMode=JsonWritter.WritterMode.APPEND)
# json格式写入
dimenSpider = Spider(parser=DimensionParser(), writter=dimenInfoWritter)
# 运行爬虫
dimenSpider.run(dimenRequests)
# 数据写入文件
dimenSpider.write(file_key+'dimen.xlsx')
# dimenSpider.write(file_key+'dimen.json')
