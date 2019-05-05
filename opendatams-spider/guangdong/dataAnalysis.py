# # -*- coding: utf-8 -*-
# # author: pengr

from sspider import XlsxWritter
from datacommon import reader, DataSet


filekey = '湛江'
date_format_key = '%Y-%m-%d'


def perfection(data):
    # 单个文件完整性统计
    result = data.runUDF('DropInvalidColumnsUDF').count().runUDF(
        'PerfectionUDF', nrows=data.nrows)
    return result.data[0][-1]


def dateFormat(data, format=date_format_key):
    # 获取时间格式列
    columns = data.runUDF('GetDateColumns').distinct()
    new_col = []
    for col in columns.data[0]:
        if len(col) > 2:
            col = col[1:-1]
            new_col.extend([int(i) for i in col.split(',')])

    # 判断时间格式
    date_result = data.runUDF('IsTruthDateFormatUDF', *new_col, format=format)
    # print(date_result)
    date_per = date_result.count().runUDF('PerfectionUDF', nrows=date_result.nrows)

    if len(new_col) == 0:
        return '未发现日期格式数据列', -1
    return '日期格式列： {}, 日期格式合格率: {}' .format(','.join([str(i) for i in new_col]), date_per.data[0][-1]), date_per.data[0][-1]


def avg(items):
    # 数据集完整性统计
    return sum(items)/len(items)


print('-'*20, 'start', '-'*20)
# 根据存储的下载信息进行读取文件
downloadInfoFile = '湛江数据下载信息.xlsx'
downloadInfo = reader.getFileData(downloadInfoFile)


import os
# 读取source文件夹下所有文件
# def listFile(dir='source'):
#     file_list = []
#     for root, dirs, files in os.walk('source'):
#         for file in files:
#             file_list.append(os.path.join(root, file))
#     return file_list


downloadInfo[0].append('完整率')
downloadInfo[0].append('日期格式判定('+date_format_key+')')
items_perfaction = []
dates_percentage = []
# 通过文件路劲读取文件，计算完整率
for item in downloadInfo[1:]:
    path = item[6]
    # 读取数据
    data = reader.getFileData(path)
    data = DataSet(data)
    # 单个文件完整率
    item_perfaction = perfection(data)

    # 时间格式判断
    date_format, date_format_per = dateFormat(data)

    print(
        '{:<20} --完整率： {:<10f} , -- {:<}'.format(item[3], item_perfaction, date_format))

    item.append(item_perfaction)
    item.append(date_format)
    if date_format_per != -1:
        dates_percentage.append(date_format_per)
    items_perfaction.append(item_perfaction)

data_item_perfection = avg(items_perfaction)
date_format_percentage = avg(dates_percentage)

print('数据完整率：', data_item_perfection)
print('日期合格率：', date_format_percentage)

downloadInfo.append(['数据完整率：', data_item_perfection])
downloadInfo.append(['日期格式匹配率：', date_format_percentage])

writter = XlsxWritter(writeMode=XlsxWritter.WritterMode.APPEND)
writter.write(filekey+'数据统计.xlsx', data=downloadInfo, write_header=False)
