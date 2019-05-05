# 根据数据集建立目录
import os


def saveContent(path, content):
    if os.path.exists(path):
        return
    dir = os.path.dirname(path)
    if not os.path.exists(dir):
        os.makedirs(dir)
    with open(path, 'wb') as f:
        f.write(content)


# 判断空数据
def isBlank(item, NonKeys=['NONE', 'NULL', 'NON']):
    if item is None or len(str(item)) == 0 or str(item).strip().upper() in NonKeys:
        return True
    for ch in str(item):
        if ch != ' ':
            return False
    return True

# 统计非法日期格式格式数据条目/所占比


import time

# 判断日期格式


def isDateTrueFormat(date, format='%Y-%m-%d'):
    try:
        time.strptime(date, "%Y-%m-%d")
        return True
    except:
        return False


# 通过类名获取对象
def createInstance(module_name, class_name, *args, **kwargs):
    module_meta = __import__(module_name, globals(), locals(), [class_name])
    class_meta = getattr(module_meta, class_name)
    obj = class_meta(*args, **kwargs)
    return obj
