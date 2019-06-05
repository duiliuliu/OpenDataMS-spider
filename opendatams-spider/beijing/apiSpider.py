from sspider import Spider, Request, RequestManager, HtmlParser, XlsxWritter, JsonWritter
import requests
import time

from datacommon.util import saveContent

host = 'http://data.beijing.gov.cn'
token = '1559560611255'


class cataParser(HtmlParser):

    def parse(self, response):
        items = response.json()['response']['docs']
        data = []
        for item in items:
            data.append({
                '名称': item['title'],
                '机构': item['unitName'],
                '调用次数': item['callCount'],
                '下载次数': item['downloadCount'],
                'url': item['indexUrl'].replace('D:/tomcat7054/apache-tomcat-7.0.54/webapps/publish/bjdata', host),
                '描述': ';'.join(item['content']),
                '更新日期': item['publishDateStr'],
                'size': item['size'],
                'id': item['indexUrl'].split('/')[-1].replace('.html', '').replace('.htm', '')
            })
        return [], data


cataWritter = XlsxWritter(writeMode=XlsxWritter.WritterMode.EXTEND)
cataSpider = Spider(name="cataSpider",
                    parser=cataParser(), writter=cataWritter)


class dimenParser(HtmlParser):
    # http://data.beijing.gov.cn/cms/web/APIInterface/dataDoc.jsp?contentID=10785
    def parse(self, response):
        doc = response.html()
        data = response.request.other_info

        url = doc.xpath('/html/body/div[1]/div[2]/div[2]/p/a')[0].text
        param = doc.xpath(
            '/html/body/div[1]/div[2]/div[4]/table/tbody/tr/td[4]')[0].text
        des = doc.xpath(
            '/html/body/div[1]/div[2]/div[6]/p')[0].xpath('string(.)')

        data['apiurl'] = url.replace('个人唯一标识码', token).replace('文件编号', param)
        data['apidoc'] = des.strip()

        # 测试api
        test = 'API测试'
        try:
            res = requests.get(data['apiurl'])
            path = 'source/'+data['名称']+'.json'
            saveContent(path, res.content)
            data[test] = '可用'
            data[test+'_status'] = res.status_code
            data[test+'_text'] = res.text
        except Exception as e:
            data[test] = '异常: '+str(e)

        return [], data


dimenWritter = XlsxWritter(writeMode=XlsxWritter.WritterMode.APPEND)
dimenSpider = Spider(name="dimenSpider",
                     parser=dimenParser(), writter=dimenWritter)


def getParam(start):
    start *= 10
    return {
        'q': '_text_:*',
        'wt': 'json',
        'rows': '10',
        'start': start,
        'enableElevation': 'true',
        'forceElevation': 'true',
        'sort': 'publishDate desc',
        'fl': '_uuid_,title,content,indexUrl,pubDateStr,pubDate,publishDate,publishDateStr,size,score,unitName,downloadCount,callCount,imgsrc,[elevated],imgsrc',
        'fq': ''
    }


if __name__ == '__main__':

    filekey = '全部api'

    # 共1238条数据接口，124页
    reqs = [Request('post', 'http://data.beijing.gov.cn/search/1_file/elevate',
                    data=getParam(i)) for i in range(124)]

    cataSpider.run(reqs)
    cataSpider.write(filekey+'catalog.xlsx', write_header=True)

    cata = cataSpider.getItems(type='dict')
    # print([i for i in cata])

    doc_url = 'http://data.beijing.gov.cn/cms/web/APIInterface/dataDoc.jsp?contentID='
    # 根据目录构建请求,method='get',url,other_info为request可携带存储的与网络请求无关的信息，此处存储数据集名称
    dimenRequests = [Request('get', doc_url + i['id'], other_info=i)
                     for i in cata]

    # 运行爬虫
    dimenSpider.run(dimenRequests)
    # 数据写入文件
    dimenSpider.write(filekey+'dimen.xlsx', write_header=True)
