# -*- coding: utf-8 -*-
# author: pengr

import util
import constants

non = constants.NonData()


class BaseUDF(object):
    # 用户自定义函数

    def evaluate(self, *args, **kwargs):
        raise NotImplementedError()


class TestUDF(BaseUDF):

    def evaluate(self, *args, **kwargs):
        print('test udf and params is arg: '+args)


class IsNonDataUDF(BaseUDF):
    # 统计数据集中空数据条目/所占比例

    def evaluate(self, *items):
        res = []
        for item in items:
            if util.isBlank(item):
                res.append(non)
            else:
                res.append(item)
        return res


class IsTruthDateFormatUDF(BaseUDF):
    # 统计日期格式
    def evaluate(self, col):
        if util.isDateTrueFormat(col):
            return col
        return non
