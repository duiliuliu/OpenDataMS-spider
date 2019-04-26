# -*- coding: utf-8 -*-
# author: pengr

from util import createInstance

# 对相应的类容执行函数，


class Task(object):

    def __init__(self, data):
        self.data = data

    def runUDF(self, UDfunc, *cols, **kwargs):
        moduleName = kwargs['module'] if kwargs['module'] else 'udfs'
        UDfunc = createInstance(moduleName, UDfunc)
        items = []
        for item in self.data:
            params = []
            for col in cols:
                params.append(item[col])
            items.append(UDfunc.evaluate(*params))
        self.data = items
        return self

    def filter(self, **kwcols):
        self.runUDF('FilterFunc', module='task', **kwcols)
        return self

    def count(self, *cols):
        res = {col: 0 for col in cols}
        for item in self.data:
            for col in cols:
                res[col] = res[col]+1 if item[col] is not None else res[col]
        self.data = [res[col] for col in cols]
        return self

    def min(self, *cols):
        res = {col: self.data[col] for col in cols}
        for item in self.data:
            for col in cols:
                res[col] = item[col] if item[col] > res[col] else res[col]
        self.data = [res[col] for col in cols]
        return self

    def max(self, *cols):
        res = {col: self.data[col] for col in cols}
        for item in self.data:
            for col in cols:
                res[col] = item[col] if item[col] < res[col] else res[col]
        self.data = [res[col] for col in cols]
        return self

    @property
    def result(self):
        return self.data


class CommonFunc(object):
    def evaluate(self, *args, **kwargs):
        raise NotImplementedError()


class FilterFunc(CommonFunc):

    def evaluate(self,  **kwcols):
        pass


class MinFunc(CommonFunc):
    def evaluate(self, *args, **kwargs):
        return super().evaluate(*args, **kwargs)
