# # -*- coding: utf-8 -*-
# # author: pengr

from sspider import XlsxWritter, TxtWritter, Logger
from datacommon import reader, DataSet


def perfection(data):
    # 单个文件完整性统计
    # @param: DateSet
    result = data.runUDF('DropInvalidColumnsUDF').count().runUDF(
        'PerfectionUDF', nrows=data.nrows)
    return result.data[0][-1]


def dateFormat(data, format='%Y-%m-%d'):
    # 获取时间格式列
    # @param: DateSet,str
    columns = data.runUDF('GetDateColumns').distinct()
    new_col = []
    for col in columns.data[0]:
        if len(col) > 2 and type(col) == str:
            col = col[1:-1]
            new_col.extend([int(i) for i in col.split(',')])
        elif type(col) == list:
            new_col.extend(col)
        else:
            print('-------------', col)

    # 判断时间格式
    date_result = data.runUDF('IsTruthDateFormatUDF', *new_col, format=format)
    # print(date_result)
    date_per = date_result.count().runUDF('PerfectionUDF', nrows=date_result.nrows)

    if len(new_col) == 0:
        return '未发现日期格式数据列', -1
    return '日期格式列： {}, 日期格式合格率: {:<.2%}' .format(','.join([str(i) for i in new_col]), date_per.data[0][-1]), date_per.data[0][-1]


def avg(items):
    # 均值计算--数据集完整性统计
    # @param: list
    return sum(items)/len(items)


def fileAnalysis(filename, date_format):
    # 数据文件质量分析
    # @param: str,str
    data = reader.getFileData(filename)
    data = DataSet(data)
    # 完整率
    item_perfaction = perfection(data)

    # 时间格式判断
    date_format, date_format_per = dateFormat(
        data, format=date_format)

    # 时效性

    # 是否满足唯一性
    unique = 1 if data.nrows == data.distinct().nrows else 0

    result = [item_perfaction, date_format, date_format_per, unique]

    key = filename if len(filename) < 40 else filename[:40]+'...'
    print('{}  -- 完整率：{:<.2%} - 时间： {} {} - 唯一性： {}'.format(key, *result))

    return result


import os


def listFile(dir='source'):
    # 读取source文件夹下所有文件
    # @param: str
    file_list = []
    for root, _, files in os.walk(dir):
        for file in files:
            file_list.append(os.path.join(root, file))
    return file_list


if __name__ == '__main__':

    # 文件路径
    path_str = 'source'
    filekey = '全部'
    date_format_key = '%Y-%m-%d'

    log = Logger(filekey+'-'+__file__.split('.')[0])

    # 通过文件路径访问文件

    # 根据存储的下载信息进行读取文件路径
    # downloadInfoFile = filekey+'数据下载信息.xlsx'
    # downloadInfo = reader.getFileData(downloadInfoFile)
    # downloadInfo[0].append('完整率')
    # downloadInfo[0].append('日期格式判定('+date_format_key+')')

    # 根据路径获取路径下的所有文件
    downloadInfo = [[os.path.basename(i), i]
                    for i in listFile(dir=path_str)]
    downloadInfo.insert(
        0, ['名称', '路径', '完整率', '日期格式判定('+date_format_key+')', '唯一性'])

    items_perfaction = []
    dates_percentage = []

    error = []

    # 读取信息中路径索引列
    path_index = 1
    # 读取信息中文件名索引列
    name_index = 0

    # 通过文件路劲读取文件，计算完整率
    for item in downloadInfo[1:]:
        path = item[path_index]
        # 读取数据
        try:
            item.extend(fileAnalysis(path, date_format_key))
        except Exception as e:
            log.exception(path + '==='+str(e))

    writter = XlsxWritter(writeMode=XlsxWritter.WritterMode.APPEND)
    writter.write(filekey+'数据统计.xlsx', data=downloadInfo, write_header=False)
