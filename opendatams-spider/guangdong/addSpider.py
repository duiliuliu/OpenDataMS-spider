

# 补充数据集

from cataSpider import cataSpider, getCataIndex
from datacommon import reader
from sspider import XlsxWritter, Request
from dataSpider import dimenSpider
import json
import demjson

allInfos = set()
hasInfos = set()
filename = '全部dimen.xlsx'
downloadInfoFile = '全部数据下载信息.xlsx'


def listToDict(item, header):
    temp = {}
    for i in range(len(header)):
        temp[header[i]] = item[i]
    return temp


cata = getCataIndex('all', 'all')

# 获取所有数据集信息
itemInfos = []
for item in cata:
    itemInfos.extend(item['items'])


fileItems = reader.getFileData(filename)
header = fileItems[0]
hasItems = []

for i in itemInfos:
    allInfos.add(i['url'])
for i in fileItems:
    hasInfos.add(i[0])

needInfos = allInfos - hasInfos
print('needInfos', '---', len(needInfos))

reqs = []
downloadinfo = []

for i in fileItems:
    itemDict = listToDict(i, header)
    if not itemDict['数据下载']:
        reqs.append(Request('get', itemDict['url'], other_info=itemDict))
    else:
        hasItems.append(itemDict)

# print('reqs', '---', len(reqs))
# for i in needInfos:
#     for item in itemInfos:
#         if i == item['url']:
#             itemDict = item
#             reqs.append(Request('get', itemDict['url'], other_info=itemDict))
#             break

print('reqs', '---', len(reqs))

print('hasItems', '---', len(hasItems))


# 运行爬虫
dimenSpider.run(reqs)
# 数据存储
for i in dimenSpider.getItems(type='dict'):
    hasItems.append(i)

xlsx = XlsxWritter(XlsxWritter.WritterMode.EXTEND)
xlsx.write_buffer(hasItems)
xlsx.write('补全'+filename,write_header=False)


for i in hasItems[1:]:
    try:
        downloadinfo.extend(demjson.decode(i['数据下载'].replace(
        'True', '\'True\'').replace('False', '\'False\'')))
    except Exception as e:
        if e.__str__ ()== 'String literal is not terminated':
            downloadinfo.extend(demjson.decode(i['数据下载'].replace(
        'True', '\'True\'').replace('False', '\'False\'')+'数据集内容过多导致名称丢失\'}]'))
    
xlsx = XlsxWritter(XlsxWritter.WritterMode.EXTEND)
xlsx.write_buffer(downloadinfo)
xlsx.write(downloadInfoFile, write_header=True)
