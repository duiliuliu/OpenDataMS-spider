from sspider import Spider, Request, RequestManager, HtmlParser, XlsxWritter, JsonWritter
import requests

# 如何运行该爬虫：
# 下载安装python环境
# 下载安装爬虫库 sspider : 使用该命令即可`pip install sspider`

# 广州开放数据 数据集request,page:163
# type/0 数据集
# type/1 api

# range(1) 表示抓取1页，数据集共有163页，如需拆去全部数据，请改为163
reqs = [Request(
    'get', 'http://www.gddata.gov.cn/index.php/data/ls/type/0/p/{}.html'.format(i)) for i in range(1)]

seed_url = 'http://www.gddata.gov.cn'

# 获取数据集目录的解析器


class CatalogParser(HtmlParser):
    def parse(self, response):
        doc = response.html()
        urls = doc.xpath(
            '/html/body/div[1]/div[2]/div[3]/div[1]/div/div[2]/ul/li/p[1]/a')
        data = [[seed_url + url.get('href'), url.xpath('string(.)')]
                for url in urls]
        return [], data


# 数据写入类，此处使用xlsx格式数据写入
writter = XlsxWritter(writeMode=XlsxWritter.WritterMode.EXTEND)
writter.insert(['数据集url', '数据集名称'])

# 建立爬虫对象，解析类是CatalogParser
cataSpider = Spider(parser=CatalogParser(), writter=writter)
cataSpider.run(reqs)
cataSpider.write('catalog.xlsx')

############################################################################

# 根据目录信息继续抓取
# 获取目录信息
cata = cataSpider.getItems()[1:]

# 根据目录构建请求,method='get',url,other_info为request可携带存储的与网络请求无关的信息，此处存储数据集名称
dimenRequests = [Request('get', i[0], other_info=i[1]) for i in cata]

# 数据集详细页面有以下几种情况：
# 1. 该网页不存在，被重定向
# 2. 网页无数据集下载，但是有数据预览
# 3. 网页无数据集下载，也无数据预览 -- 基本与第一种一致，会直接重定向


# 数据集开放维度及其详细粒度支持：
# 1. 数据下载；a.xls格式数据下载 b.csv c.json
# 2. 数据预览 or 数据管理； 是否可以预览
# 3. 开放协议  开放协议是否详细
# 4. 数据分析   是否可以数据分析

# 维度常量
DATA_DOWNLOAD = '数据下载'
DATA_PREVIEW = '数据预览'
DATA_MANAGE = '数据管理'
OPEN_PROTOCOL = '开放协议'
DATA_ANALYSIS = '数据分析'

# 对网页中的维度解析


class DimensionParser(HtmlParser):
    def parse(self, response):
        doc = response.html()
        # 建立维度集合
        dimensions = {a.text.strip(): a.text.strip() for a in doc.xpath(
            '/html/body/div[1]/div[2]/div[3]/div[1]/div[2]/div[1]/a')}
        # 详细维度
        if DATA_DOWNLOAD in dimensions:
            links = doc.xpath(
                '/html/body/div[1]/div[2]/div[3]/div[1]/div[2]/div[2]/ul/li//a')
            texts = doc.xpath(
                '/html/body/div[1]/div[2]/div[3]/div[1]/div[2]/div[2]/ul/li//span/text()')

            dimensions[DATA_DOWNLOAD] = []
            for i in range(len(links)):
                item = {
                    'url': seed_url+links[i].get('href'),
                    'format': texts[i] if i < len(texts) else '',
                    'name':  links[i].text.strip()
                }
                # 测试链接获取粒度值：是否可下载
                test_link = item['url']
                res = requests.get(test_link)
                if res.status_code == 200 and res.text:
                    item['是否可用'] = True
                else:
                    item['是否可用'] = False
                # 打印每个数据子项信息
                # print(item)
                dimensions[DATA_DOWNLOAD].append(item)

        # 其他维度类似

        # 返回数据集合进行存储
        data = {
            "数据集名称": response.request.other_info,
            "数据集url": response.url
        }
        data.update(dimensions)

        # 返回下一次爬取的请求集合与解析到的数据
        return [], data


# 将数据写进xlsx中，因为数据有多层嵌套，所以此时选择写入json文件中，如需要写入xlsx中，取消下方注释即可
# dimenInfoWritter = XlsxWritter(writeMode=XlsxWritter.WritterMode.APPEND)
dimenInfoWritter = JsonWritter(writeMode=JsonWritter.WritterMode.APPEND)
# json格式写入
dimenSpider = Spider(parser=DimensionParser(), writter=dimenInfoWritter)
# 运行爬虫
dimenSpider.run(dimenRequests)
# 在数据首部插入数据头
dimenInfoWritter.insert(dimenInfoWritter.headers)
# 数据写入文件
dimenSpider.write('dimen.json')
