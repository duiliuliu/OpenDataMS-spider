from sspider import Spider, Request, RequestManager, HtmlParser, XlsxWritter, JsonWritter

from datacommon import util

seed_url = 'http://www.gddata.gov.cn'

# 爬取分类信息。主题选择分类、省级部门分类、地市地区分类


# 主题分类
class TopicClassifyParser(HtmlParser):
    def parse(self, response):
        doc = response.html()
        topicCata = doc.xpath(
            '/html/body/div[1]/div[2]/div[3]/div[1]/div/div[1]/ul[1]/li/a')
        data = []
        for item in topicCata:
            data.append({
                'name': item.text,
                'url': item.get('href'),
                'num': item.text.split('(')[-1][:-1]
            })
        return [], data


topicCataWritter = XlsxWritter(XlsxWritter.WritterMode.EXTEND)
topicCataSpider = Spider(name='topicspider', parser=TopicClassifyParser(),
                         writter=topicCataWritter)


# 地市地区分类
class CityCataParser(HtmlParser):
    def parse(self, response):
        doc = response.html()
        topicCata = doc.xpath(
            '/html/body/div[1]/div[2]/div[3]/div[1]/div/div[1]/ul[3]/li/a')
        data = []
        for item in topicCata:
            data.append({
                'name': item.text,
                'url': item.get('href'),
                'num': item.text.split('(')[-1][:-1]
            })
        return [], data


cityCataWritter = XlsxWritter(XlsxWritter.WritterMode.EXTEND)
cityCataSpider = Spider(name='cityspider', parser=CityCataParser(),
                        writter=cityCataWritter)


# 省级部门分类
class AgencyCataParser(HtmlParser):
    def parse(self, response):
        doc = response.html()
        topicCata = doc.xpath(
            '/html/body/div[1]/div[2]/div[3]/div[1]/div/div[1]/ul[2]/li/a')
        data = []
        for item in topicCata:
            data.append({
                'name': item.text,
                'url': item.get('href'),
                'num': item.text.split('(')[-1][:-1]
            })
        return [], data


agencyCataWritter = XlsxWritter(XlsxWritter.WritterMode.EXTEND)
agencyCataSpider = Spider(name='agencyspider', parser=AgencyCataParser(),
                          writter=agencyCataWritter)

# 获取数据集目录的解析器


class CatalogParser(HtmlParser):
    def parse(self, response):
        doc = response.html()
        block_path = '/html/body/div[1]/div[2]/div[3]/div[1]/div/div[2]/ul/li'
        blocks = doc.xpath(block_path)

        info = response.request.other_info
        items = []
        for i in range(1, len(blocks)+1):
            labels_preview = doc.xpath(
                block_path+'[{}]/p/label/text()'.format(i))
            labels_foramt = doc.xpath(
                block_path+'[{}]/p/span/label/text()'.format(i))
            item = {}
            for key in info:
                item[key] = info[key]
            item.update({
                '数据集url': seed_url + doc.xpath(block_path+'[{}]/p[1]/a/@href'.format(i))[0],
                '数据集name': doc.xpath(block_path+'[{}]/p[1]/a/text()'.format(i))[0].strip(),
                '已下载次数': doc.xpath(block_path+'[{}]/p/span[3]/text()'.format(i))[0].strip(),
                '访问量': doc.xpath(block_path+'[{}]/p/span[4]/text()'.format(i))[0].strip(),
                '预览标签': ','.join(labels_preview),
                '格式标签': ','.join(labels_foramt)
            })
            items.append(item)

        nextReqNode = doc.xpath(
            '/html/body/div[1]/div[2]/div[3]/div[1]/div/div[2]/div[2]/div/a[@class="next"]')

        if nextReqNode and len(nextReqNode) > 0:
            nextRequest = [Request(
                'get', seed_url+nextReqNode[0].get('href'), other_info=info)]
        else:
            nextRequest = []

        return nextRequest, items


# 数据写入类，此处使用xlsx格式数据写入
writter = XlsxWritter(writeMode=XlsxWritter.WritterMode.EXTEND)

# 建立爬虫对象，解析类是CatalogParser
cataSpider = Spider(name='cataSpider', parser=CatalogParser(), writter=writter)


if __name__ == '__main__':
    req = Request(
        'get', 'http://www.gddata.gov.cn/index.php/data/ls/type/0.html')

    # 主题分类
    topicCataSpider.run(req)
    catalog = topicCataSpider.getItems(type='dict')
    reqs = [Request('get', seed_url+i['url'], other_info=i)
            for i in catalog if '全部' not in i['name']]
    cataSpider.run(reqs)
    cataSpider.write('topicCata.xlsx', write_header=True)
    cataSpider.writter.remove_buffer()

    # 地市地区分类
    cityCataSpider.run(req)
    catalog = cityCataSpider.getItems(type='dict')
    reqs = [Request('get', seed_url+i['url'], other_info=i)
            for i in catalog if '全部' not in i['name']]
    cataSpider.run(reqs)
    cataSpider.write('cityCata.xlsx', write_header=True)
    cataSpider.writter.remove_buffer()

    # 省级部门分类
    agencyCataSpider.run([Request(
        'get', 'http://www.gddata.gov.cn/index.php/data/ls/type/0/u/{}.html'.format(i)) for i in range(1, 6)])
    catalog = agencyCataSpider.getItems(type='dict')
    reqs = [Request('get', seed_url+i['url'], other_info=i)
            for i in catalog if '全部' not in i['name']]
    cataSpider.run(reqs)
    cataSpider.write('agencyCata.xlsx', write_header=True)
    cataSpider.writter.remove_buffer()
