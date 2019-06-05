from sspider import Spider, Request, RequestManager, HtmlParser, XlsxWritter, JsonWritter
import requests
import time

from datacommon.util import saveContent

host = 'http://www.gyopendata.gov.cn'
token = '180a494e8bc7ffc505151ef25f347551'


class cataParser(HtmlParser):

    def parse(self, response):
        doc = response.html()
        data = []
        # /html/body/div[5]/table/tbody/tr/td[3]/div[2]
        items = doc.xpath(
            '/html/body/div[5]/table/tbody/tr/td[3]/div/div[1]/a')[1:6]
        for item in items:
            data.append({
                'url': host+item.get('href'),
                '名称': item.text.strip()
            })

        return [], data


cataWritter = XlsxWritter(writeMode=XlsxWritter.WritterMode.EXTEND)
cataSpider = Spider(name="cataSpider",
                    parser=cataParser(), writter=cataWritter)


class dimenParser(HtmlParser):
    def parse(self, response):
        doc = response.html()
        data = response.request.other_info

        trs = doc.xpath('/html/body/div[5]/div/div[2]/div/div[1]/table/tr')

        for td in trs:
            item = td.xpath('string(.)').strip()
            temp = item.split('\r\n')
            if len(temp) >= 2:
                data[temp[0].strip()] = temp[-1].strip()
            # 分割

        # 测试api
        test = 'API测试'
        try:
            headers = {
                'Authorization': token
            }
            res = requests.get(data['请求示例：'], headers=headers)
            file_format = '.json' if 'json' in data['支持格式：'] else '.'+data['支持格式：']
            path = 'source/'+data['名称']+file_format
            saveContent(path, res.content)
            data[test] = '可用'
            data[test+'_status'] = res.status_code
            data[test+'_text'] = res.text
        except Exception as e:
            data[test] = '异常: '+str(e)
            if '请求示例：' not in data:
                data[test] = '异常: 无请求示例'

        return [], data


dimenWritter = XlsxWritter(writeMode=XlsxWritter.WritterMode.APPEND)
dimenSpider = Spider(name="dimenSpider",
                     parser=dimenParser(), writter=dimenWritter)


def getParam(page):
    return {
        'type': '2',
        'value1': '0',
        'pageNo': page
    }


if __name__ == '__main__':
    filekey = '全部api'

    # 共480条数据接口，97页
    reqs = [Request('get', 'http://www.gyopendata.gov.cn/city/apiservice.htm',
                    params=getParam(i)) for i in range(1, 97)]

    cataSpider.run(reqs)
    cataSpider.write(filekey+'catalog.xlsx', write_header=True)

    cata = cataSpider.getItems(type='dict')
    # print([i for i in cata])

    # 根据目录构建请求,method='get',url,other_info为request可携带存储的与网络请求无关的信息，此处存储数据集名称
    dimenRequests = [Request('get',  i['url'], other_info=i)
                     for i in cata]

    # 运行爬虫
    dimenSpider.run(dimenRequests)
    # 数据写入文件
    dimenSpider.write(filekey+'dimen.xlsx', write_header=True)
