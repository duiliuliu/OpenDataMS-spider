from sspider import Request, HtmlParser, Spider, CommonWritter
import re
import requests


class ipParser(HtmlParser):

    def parse(self, response):
        doc = response.text
        pattern = '(?:\d+\.){3}\d+:\d+'
        ips = re.findall(pattern, doc)
        return [], ips


class ProxyPool():

    def __init__(self, *args, **kwargs):
        self.__pool = []
        self.idx = 0
        self.initPool()

    def initPool(self):
        headers = {
            # 'Cookie': '__jsluid=7047e65c298237d485207bb867f6d903; __jsl_clearance=1556350912.141|0|ObnYAjOyNX3tzLrsd9c%2Btx7qzRk%3D; Hm_lvt_1761fabf3c988e7f04bec51acd4073f4=1556350916; Hm_lpvt_1761fabf3c988e7f04bec51acd4073f4=1556351251',
            # 'Host': 'www.66ip.cn',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.108 Safari/537.36',
        }
        reqs = [
            Request('get', 'http://www.66ip.cn/mo.php?sxb=&tqsl=100&port=&export=&ktip=&sxa=&submit=%CC%E1++%C8%A1&textarea=', headers=headers),
            Request(
                'get', 'http://www.89ip.cn/tqdl.html?api=1&num=100&port=&address=&isp=', headers=headers)
        ]

        spider = Spider(parser=ipParser(), writter=CommonWritter(
            writeMode=CommonWritter.WritterMode.EXTEND))
        spider.run(reqs)

        # self.__pool = [{'http': i} for i in spider.getItems()]
        # # https://tool.lu/ip/ajax.html
        for i in spider.getItems():
            proxy = {'http': i}
            try:
                timeout = 10
                res = requests.get('http://2019.ip138.com/ic.asp',
                                   proxies=proxy, timeout=timeout)
                group = re.search(
                    '<center>.*?((?:\d+\.){3}\d+).*?</center>', res.text)
                if group:
                    print(group[0], "---------", proxy['http'])
                    self.__pool.append(proxy)
                else:
                    print(group)
                    print('invalid', proxy['http'])
            except:
                print('invalid and exception', proxy['http'])

    @property
    def pool(self):
        return self.__pool

    def check(self, host):
        for proxy in self.__pool:
            try:
                res = requests.get(host, proxy=proxy)
                if res.status_code != 200:
                    self.pop(proxy)
            except:
                self.pop(proxy)

    def push(self):
        if len(self.__pool) > 0:
            self.idx %= len(self.__pool)
            return self.pool[self.idx]

    def pop(self, proxy):
        self.__pool.pop(self.__pool.index(proxy))


if __name__ == '__main__':
    ppool = ProxyPool()
    print(ppool.push())
