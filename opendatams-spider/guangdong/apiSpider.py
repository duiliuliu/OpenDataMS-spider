from sspider import Spider, Request, RequestManager, HtmlParser, XlsxWritter, JsonWritter
import requests
import time

from datacommon.util import saveContent


seed_url = 'http://www.gddata.gov.cn'

# 获取数据集目录的解析器


class DimensionParser(HtmlParser):
    def parse(self, response):
        doc = response.html()
        data = response.request.other_info

        # 创建时间/更新时间
        table = doc.xpath(
            '//div[@class="base-info"]/table')[0]
        for tbody in table:
            tds = tbody.getchildren()
            for index in range(0, len(tds), 2):
                data[tds[index].text.strip()] = tds[index+1].text

        # api相关
        block_path = '/html/body/div[1]/div[2]/div[3]/div[1]/div[2]/div[2]/ul/li'

        # 获取key获取值
        # key是第一个子节点的text
        # value所有text
        block = doc.xpath(block_path)
        for li in block:
            tags = []
            for i in li:
                if i.tag == 'span'or 'h3':
                    tags.append(i)
            key = tags[0].xpath('string(.)').strip()
            value = tags[1].xpath('string(.)').strip()
            data[key] = value

        # 测试API
        demo = '请求示例：'
        method = '请求方式：'
        test = 'API测试'
        if demo in data:
            start = time.time()
            try:
                if data[method].upper() == 'GET':
                    res = requests.get(data[demo])
                else:
                    res = requests.post(data[demo])
                data[test] = '可用'
                data[test+'_status'] = res.status_code
                data[test+'_text'] = res.text
                path = 'source/'+data['数据集name']+'.'+data['支持格式：']
                saveContent(path, res.content)
            except:
                data[test] = '异常'
            data['响应时长'] = time.time()-start
        else:
            data[test] = '不存在'

        # 返回下一次爬取的请求集合与解析到的数据
        return [], data


# 将数据写进xlsx中，因为数据有多层嵌套，所以此时选择写入json文件中，如需要写入xlsx中，取消下方注释即可
dimenInfoWritter = XlsxWritter(writeMode=XlsxWritter.WritterMode.APPEND)

# 创建爬虫对象
dimenSpider = Spider(
    name='dimenSpider', parser=DimensionParser(), writter=dimenInfoWritter)


if __name__ == '__main__':
    from cataSpider import cataSpider

    # 通过filekey指定不同城市或者不同政府机构，支持以下四个
    filekey = '全部api'

    # 如何运行该爬虫：
    # 下载安装python环境
    # 下载安装爬虫库 sspider : 使用该命令即可`pip install sspider`

    # type/0 数据集
    # type/1 api

    cataSpider.run(
        'http://www.gddata.gov.cn/index.php/data/ls/type/1/p/0.html')
    cataSpider.write(filekey+'catalog.xlsx', write_header=True)

    ############################################################################

    # 根据目录信息继续抓取
    # 获取目录信息
    cata = cataSpider.getItems(type='dict')
    # print([i for i in cata])

    # 根据目录构建请求,method='get',url,other_info为request可携带存储的与网络请求无关的信息，此处存储数据集名称
    dimenRequests = [Request('get', i['数据集url'], other_info=i)
                     for i in cata]
    # 运行爬虫
    dimenSpider.run(dimenRequests)
    # 数据写入文件
    dimenSpider.write(filekey+'dimen.xlsx', write_header=True)
