# 北京市的api数据请求结果关联到其相应的文件，因此需要再次下载


from datacommon import reader
from datacommon.util import saveContent, listFile
import json
import requests
import os


def filterNullList(tar_list):
    for i in tar_list:
        if i:
            return i


files = listFile('source')

for file in files:
    content = json.loads(reader.getTxtData(file).strip())
    url = content['result']['address']
    if os.path.exists('apidatasource/'+content['result']
                      ['name']+'.'+url.split('.')[-1]):
        continue
    res = requests.get(url)
    print('-----'+content['result']['name'])
    saveContent('apidatasource/'+content['result']
                ['name']+'.'+url.split('.')[-1], res.content)
