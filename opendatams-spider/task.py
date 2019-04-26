# -*- coding: utf-8 -*-
# author: pengr

from util import createInstance
from constants import NonData

# 对相应的类容执行函数，

nondata = NonData()


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

    def filterNonData(self, col):
        items = []
        for item in self.data:
            if item[col] != nondata:
                items.append(item[col])
        self.data = items
        return self

    def count(self, *cols):
        res = [0 for col in cols]
        for item in self.data:
            for idx in range(len(cols)):
                res[idx] = res[idx]+1 if item[cols[idx]
                                              ] != nondata else res[idx]
        self.data = res
        return self

    def min(self, *cols):
        res = [nondata for col in cols]
        for item in self.data:
            for idx in range(len(cols)):
                res[idx] = item[cols[idx]] if res[idx] == nondata or item[cols[idx]
                                                                          ] < res[idx] else res[idx]
        self.data = res
        return self

    def max(self, *cols):
        res = [nondata for col in cols]
        for item in self.data:
            for idx in range(len(cols)):
                res[idx] = item[cols[idx]] if res[idx] == nondata or item[cols[idx]
                                                                          ] > res[idx] else res[idx]
        self.data = res
        return self

    @property
    def result(self):
        return self.data


if __name__ == '__main__':
    numberData = [
        [1, 2, 3, 4, 5],
        [2, 4, 6, 8, 10],
        [3, 6, 9, 12, 4]
    ]

    stringData = [
        ['asda', 'asda', 'dwqwe', 'qweq'],
        ['weqcas', 'defsw', 'deqwa', 'ewqwq']
    ]

    nonTestData = [
        ['asda', 'asda', 'dwqwe', 'qweq'],
        ['weqcas', 'defsw', 'deqwa', 'ewqwq'],
        [nondata, 'none', 'null', '']
    ]

    task1 = Task(numberData)
    task1.min(1, 3)
    print(task1.result)

    task2 = Task(nonTestData)
    task2.count(0, 1, 2, 3)
    print(task2.result)
