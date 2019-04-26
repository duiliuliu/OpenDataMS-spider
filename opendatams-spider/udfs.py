# -*- coding: utf-8 -*-
# author: pengr

# 用户自定义函数


class BaseUDF(object):

    def evaluate(self, *args, **kwargs):
        raise NotImplementedError()


class TestUDF(BaseUDF):

    def evaluate(self, *args, **kwargs):
        print('test udf and params is arg: '+args)
