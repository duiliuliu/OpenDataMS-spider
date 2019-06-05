# -*- coding: utf-8 -*-
# author: pengr

# 读取文件、
# 完整性分析
# 一致性
# 日期格式

from sspider import XlsxWritter, TxtWritter
from datacommon import reader, DataSet
from datacommon.util import listFile


def avg(items):
    # 均值计算--数据集完整性统计
    # @param: list
    return sum(items)/len(items)


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
        return '日期格式一致', 1
    return '日期格式列： {}, 日期格式合格率: {:<.2%}' .format(','.join([str(i) for i in new_col]), date_per.data[0][-1]), date_per.data[0][-1]


def uniques(data):
    # 一致性
    unique = 1 if data.nrows == data.distinct().nrows else 0
    return unique


dirs = listFile('apidatasource')

res = []

for dir in dirs:
    item = {}
    try:
        data = reader.getFileData(dir)
        item['名称'] = dir
    except:
        item['异常'] = '文件解析失败！'
        continue

    try:
        data = DataSet(data)
        # 完整度
        per = perfection(data)
        # 日期一致性
        df = dateFormat(data)
        # 唯一性
        uni = uniques(data)
        result = [per, df[0], df[1], uni]

        print('{}  -- 完整率：{:<.2%} - 时间： {} {} - 唯一性： {}'.format(dir, *result))

        item['完整性'] = '{:<.2%}'.format(per)
        item['日期一致性'] = df[1]
        item['唯一性'] = uni
    except Exception as e:
        item['异常'] = str(e)

    res.append(item)

writter = XlsxWritter()
for i in res:
    writter.write_buffer(i)
writter.write('数据统计.xlsx',write_header=True)