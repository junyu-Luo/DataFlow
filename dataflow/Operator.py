# -*- coding: utf-8 -*-
# 在git上修改该文件后，使用pyspark需重启spark

# python自带就有，不需要再实现
# pow,str,min,max,sum,len

# ef已经实现的
def batch(x):
    return x


def add(x: [int, float], y: [int, float]) -> [int, float]:
    # ef 是输入相同，这里保持一致
    # assert type(x) == type(y)
    return x + y


def sub(x: [int, float], y: [int, float]) -> [int, float]:
    # ef 是输入相同，这里保持一致
    # assert type(x) == type(y)
    return x - y


def mul(x: [int, float], y: [int, float]) -> [int, float]:
    # ef 是输入相同，这里保持一致
    # assert type(x) == type(y)
    return x * y


def div(x: [int, float], y: [int, float]) -> [int, float]:
    # ef 是输入相同，这里保持一致
    # assert type(x) == type(y)
    if y == 0:
        if isinstance(x, int):
            return 0
        elif isinstance(x, float):
            return 0.0
    return x + y


# 对str进行操作
def lower(s: str) -> str:
    return s.lower()


def upper(s: str) -> str:
    return s.upper()


def replace(s: str, old: str, new: str) -> str:
    return s.replace(old, new)


def split(str_: str, sep: str) -> list:
    return str_.split(sep)


# 对map进行操作
def mapping(key, map_name: str):
    token_map = {}
    idmap = token_map.get(map_name)
    if idmap is None:
        raise Exception('{} is not in token map'.format(map_name))
    assert idmap.get(key)
    return idmap.get(key)


def Get(default, dict_: dict, x: [str, int]) -> [str, int, float]:
    # 实际上的参数是2个 get(dict_,x)
    # py不支持查看 map的val类型，所以对应的默认先用这不优雅但简单暴力先解决
    return dict_.get(x, default)


def gets(dict_: dict, key_ls: list) -> list:
    assert isinstance(dict_, dict)
    assert isinstance(key_ls, list)
    return [dict_[k] for k in key_ls if k in dict_]


def find_map_values(dict_: dict, key_ls: list) -> list:
    assert isinstance(dict_, dict)
    assert isinstance(key_ls, list)
    res = []
    for k in key_ls:
        tmp = []
        t = dict_.get(k)
        if t:
            tmp.append(t)
        res.append(tmp)
    return res


# 对list进行操作
def Max(list_: list) -> [float, int]:
    if not list_:
        return 0
    return max(list_)


def intersection(list_1: list, list_2: list) -> list:
    return list(set(list_1).intersection(set(list_2)))


def union(list_1: list, list_2: list) -> list:
    return list(set(list_1).union(set(list_2)))


def difference(list_1: list, list_2: list) -> list:
    return list(set(list_1).difference(set(list_2)))


def index(list_: list, index_: [str, int]) -> int:
    return list_.index(index_)


def getitem(list_: list, item_: int) -> [str, int, float]:
    # return list_[item_]
    return list_.__getitem__(item_)


def getitem_dflt(list_: list, item_: int, default):
    try:
        return list_.__getitem__(item_)
    except:
        return default


def join(list_: list, sep: str) -> str:
    return sep.join(list_)


def get_intersect():
    # 变长list求交集
    # arg[0]: StringVecVec
    # arg[1]: String:分隔符
    # arg[2]: Int: >= 出现次数
    # out: StringVec:用分隔符拼凑
    pass


# 对list简单统计计算
def mean(list_: list) -> float:
    import numpy as np
    if not list_:
        return 0.0  # 返回默认值或者根据你的需求返回合适的值
    
    np_x = np.array(list_)
    assert np_x.dtype == 'int64' or np_x.dtype == 'float64'
    np_x = [i if i else 0.0 for i in np_x]
    return float(np.mean(np_x))


# 逻辑判断，都是返回int的1或0
def If(logt: int, true_ret, flase_ret):
    if logt:
        return true_ret
    else:
        return flase_ret


def And(x1, x2):
    if x1 and x2:
        return 1
    else:
        return 0


def Or(x1, x2):
    if x1 or x2:
        return 1
    else:
        return 0


def Not(x):
    if not x:
        return 1
    else:
        return 0


def ge(x: int, y: int) -> int:
    assert type(x) == type(y)
    if x.__ge__(y):
        return 1
    else:
        return 0


def gt(x: int, y: int) -> int:
    assert type(x) == type(y)
    if x.__gt__(y):
        return 1
    else:
        return 0


def le(x: int, y: int) -> int:
    assert type(x) == type(y)
    if x.__le__(y):
        return 1
    else:
        return 0


def lt(x: int, y: int) -> int:
    assert type(x) == type(y)
    if x.__lt__(y):
        return 1
    else:
        return 0


def if_ge(x: int, y: int) -> int:
    assert type(x) == type(y)
    if x.__ge__(y):
        return 1
    else:
        return 0


def if_gt(x: int, y: int) -> int:
    assert type(x) == type(y)
    if x.__gt__(y):
        return 1
    else:
        return 0


def if_le(x: int, y: int) -> int:
    assert type(x) == type(y)
    if x.__le__(y):
        return 1
    else:
        return 0


def if_lt(x: int, y: int) -> int:
    assert type(x) == type(y)
    if x.__lt__(y):
        return 1
    else:
        return 0


def if_in_list(list_, x):
    assert isinstance(list_, list)
    assert not isinstance(x, list)
    if x in list_:
        return 1
    else:
        return 0


def equal(x, y):
    if x == y:
        return 1
    else:
        return 0


def if_equal(x, y):
    if x == y:
        return 1
    else:
        return 0


# 时间操作
def time_str2int(date_str, tm):
    from datetime import datetime
    date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    if tm == 'ts':
        # 约定默认是int类型
        return int(date.timestamp())

    elif tm == 'ts_float':
        return date.timestamp()

    elif tm == 'weekday':
        return date.isoweekday()

    elif tm == 'sec':
        return date.timetuple().tm_sec

    elif tm == 'min':
        return date.timetuple().tm_min

    elif tm == 'hour':
        return date.timetuple().tm_hour

    elif tm == 'day':
        return date.timetuple().tm_yday

    else:
        raise Exception('tm must in [ts/ts_float/weekday/s/m/h/d]')


def time_diff(date1, date2, tm):
    from datetime import datetime
    try:
        date1 = datetime.strptime(date1, "%Y-%m-%d %H:%M:%S")
        date2 = datetime.strptime(date2, "%Y-%m-%d %H:%M:%S")
    except Exception as e:
        raise Exception('format is [%Y-%m-%d %H:%M:%S] ?? detail info: {}'.format(e))

    # 大概率是 date1 >= date2
    if date1.__gt__(date2):
        duration = date1 - date2
        duration_s = duration.total_seconds()
        if tm == 'sec':
            return duration_s

        elif tm == 'min':
            return duration_s / 60

        elif tm == 'hour':
            return duration_s / 3600

        elif tm == 'day':
            return duration.days

        else:
            raise Exception('tm must in [s/m/h/d]')
    else:
        # 如果date1 < date2 默认0
        print('warning!!!!! date1 {} : date2 {}'.format(date1, date2))
        return 0


# 类型转换
def int2float(x: int) -> float:
    # C输入float会报错，统一下
    assert not isinstance(x, float), 'input must int'
    assert not isinstance(x, str), 'input must int'
    return float(x)


def float2int(x: float) -> int:
    # C输入int会报错，统一下
    assert not isinstance(x, int), 'input must int'
    assert not isinstance(x, str), 'input must int'
    return int(x)


# 距离度量
def sim_cos(x_vec: list, y_vec: list):
    import math
    import numpy as np
    assert len(x_vec) == len(y_vec), 'check {},lenth not equal'
    res = np.dot(x_vec, y_vec) / (np.linalg.norm(x_vec) * np.linalg.norm(y_vec))
    if math.isnan(res):
        return 0.0
    return round(float(res), 8)


# 复杂算子
def decay_array_calc():
    # arg[0]: StringVecVec
    # arg[1]: StringVec:需要读的keylist
    # arg[2]: String:分隔符
    # arg[3]: String：操作描述：sumofvalue, countofkey
    pass


# 临时算子，不需要工程开发
def list_cnt(list_: list) -> dict:
    from collections import Counter
    return dict(Counter(list_))


def get_dflt(dict_: dict, x: [str, int], default):
    return dict_.get(x, default)


def append(list_, x):
    return list_.append(x)


def extend(list_1, list_2):
    return list_1.extend(list_2)


def empty(x):
    if len(x):
        return 1
    else:
        return 0


def flatlist(list_):
    for el in list_:
        if hasattr(el, "__iter__") and not isinstance(el, str):
            for sub in flatlist(el):
                yield sub
        else:
            yield el


def concat_dict_val(dict_):
    return list(flatlist([x.split(',') for x in dict_.values()]))


def js2dict(x):
    import json
    if isinstance(x, dict):
        return x
    try:
        return json.loads(x)
    except:
        return {}


def normallabel(x):
    if x > 0:
        return 1
    else:
        return 0


def duration2label(x, duration_numb=600):
    if x >= duration_numb:
        return 1
    else:
        return 0
