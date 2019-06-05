from sspider import Spider, Request, HtmlParser, XlsxWritter, JsonWritter
import requests
import time
# from proxyPool import ProxyPool


from datacommon.util import saveContent

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
file_key = '贵州全部api--'


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
        'dataType': '1',
        '_': '1555811371572'
    }


# range(1) 表示抓取1页，数据集共有143页，如需拆去全部数据，请改为143
reqs = [Request(
    'get', url, params=getParams(i)) for i in range(1, 144)]


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
cataSpider = Spider(name="cataSpider",parser=CatalogParser(), writter=writter)
cataSpider.run(reqs)
# 数据保存
cataSpider.write(file_key+'catalog.xlsx', write_header=True)

############################################################################

# 根据目录信息继续抓取api接口信息并进行测试接口
# 获取目录信息
cata = cataSpider.getItems(type='dict')

# 代理
# ppool = ProxyPool()

url_template = 'http://data.guizhou.gov.cn/dataopen/api/dataset/{}/apidata?callback=jQuery111305121739185053189_1555829556219&_=1555829556226'

# 根据目录构建请求,method='get',url,other_info为request可携带存储的与网络请求无关的信息，此处存储数据集名称,sleep_time表示睡眠一段时间后进行请求
# 只需要目录数据以及接口信息则可以置sleep_time=0，如需要测试接口可用并下载接口数据，则需要调sleep_time为一个较大值，单位是秒，以防止网站封禁
dimenRequests = [Request('get',
                         url_template.format(i['id']), other_info=i, sleep_time=sleep_time) for i in cata]

# 数据集详细页面有以下数据需要获取：
# 1. 接口地址
# 2. 支持格式
# 3. 请求方式


# 数据集api开放维度及其详细粒度支持：
# 1. 接口地址是否可用
# 2. 支持格式是否正确
# 3. 是否有请求示例
# 4. 是否存在请求参数说明
# 5. 是否存在返回参数说明
# 6. 是否存在返回示例

# 维度常量
API_AVALIABLE = '接口可用性'
FORMAT_VALID = '格式是否匹配'
REQUEST_DEMO = '请求示例'  # 是否存在请求示例
RESPONSE_DEMO = '返回示例'  # 是否存在返回示例
REQUEST_MODE = '请求参数说明'  # 是否存在请求参数说明
RESPONSE_MODE = '返回参数说明'  # 是否存在返回参数说明


# 针对接口网页中的维度解析


class DimensionParser(HtmlParser):
    def parse(self, response):
        data = response.jsonp()['data']
        #  获取请求中保存的信息
        item = response.request.other_info
        try:
            item['接口地址'] = data['ifsAddr']
            start = time.time()
            res = requests.get(data['requestDemo'])
            if res.status_code == 200 and res.text:
                item['API_AVALIABLE'] = '可用'
                # 如需下载接口数据，去掉下方注释即可
                print(item['数据名称'],'可用')
                path = 'source/'+item['数据名称']+'.'+data['supportFormat']
                saveContent(path, res.content)
            else:
                item['API_AVALIABLE'] = '不可用'
            item['测试响应时长'] = time.time()-start
            if 'supportFormat' in data:
                item['支持格式'] = data['supportFormat']
                # 对格式验证
                # pass

            for key in data:
                if key in item:
                    item[key+'~1'] = data[key]
                else:
                    item[key] = data[key]
        except Exception as e:
            item['API_AVALIABLE'] = '接口详情页面异常'+str(e)
        # 返回下一次爬取的请求集合与解析到的数据
        return [], item


# 将数据写进xlsx中，因为数据有多层嵌套，所以此时选择写入xlsx文件中，如需要写入json中，取消下方注释即可
dimenInfoWritter = XlsxWritter(writeMode=XlsxWritter.WritterMode.APPEND)
# dimenInfoWritter = JsonWritter(writeMode=JsonWritter.WritterMode.APPEND)
# json格式写入
dimenSpider = Spider(name='dimenspider',parser=DimensionParser(), writter=dimenInfoWritter)
# 运行爬虫
dimenSpider.run(dimenRequests)
# 数据写入文件
dimenSpider.write(file_key+'dimen.xlsx',write_header=True)
# dimenSpider.write('dimen.json')
