import multiprocessing

def fun_1(parameter):
    pass

def fun_2(parameter):
    pass

def fun_3():
    pass
def fun_4():
    pass

parameter=None
p1 = multiprocessing.Process(target=fun_1, args=(parameter,))
p2 = multiprocessing.Process(target=fun_2, args=(parameter,))
p1.start()
p2.start()
p1.join()
fun_3()
p2.join()
fun_4()
"""
进程执行的路线图
fun_1 | fun_2
  |       |
  v       v
fun_3 | fun_4


"""














from concurrent.futures import ProcessPoolExecutor

a_very_big_list = []

for item in a_very_big_list:
    """
    你需要确保这个for循环每一轮之间是互不影响的， 否则无法进行并行化的处理
    some codes include:
    1. your logic functions
    2. some parameters (read only)
    3. some variables (you want to get or return)
    """
    parameters = None
    variables = None


def _fun(list_split, parameters):
    _variables = []
    for item in list_split:
        _variables = parameters

    return _variables


def fun(a_very_big_list, parameters=None, workers=8):
    list_split = []
    step = int(len(a_very_big_list) / workers)
    for i in range(workers):
        if i != workers - 1:
            # print('slice: ', i * step, ' ', (i + 1) * step)
            split = a_very_big_list[i * step:(i + 1) * step]
        else:
            # print('slice: ', i * step)
            split = a_very_big_list[i * step:]
        list_split.append(split)

    variables = []
    print("len(wblog_content_split): ", len(list_split))
    with ProcessPoolExecutor(max_workers=workers) as executor:
        for _variables in executor.map(_fun,
                                       list_split,
                                       [parameters for i in range(workers)]):
            """
            接下来你需要把每一个进程返回的结果进行组装，组装的方式要根据具体的情况灵活设计，
            例如对于不受影响的dic，可以使用dic.update
            对于list,可以使用+进行拼接
            """
            variables = variables + _variables
    return variables

# # -*- coding: utf-8 -*-
# __author__ = 'rubinorth'
#
# import itertools
#
# class MapReduce:
#     __doc__ = '''提供map_reduce功能'''
#
#     @staticmethod
#     def map_reduce(i, mapper, reducer):
#         """
#         map_reduce方法
#         :param i: 需要MapReduce的集合
#         :param mapper: 自定义mapper方法
#         :param reducer: 自定义reducer方法
#         :return: 以自定义reducer方法的返回值为元素的一个列表
#         """
#         intermediate = []  # 存放所有的(intermediate_key, intermediate_value)
#         for (key, value) in i.items():
#             intermediate.extend(mapper(key, value))
#
#         # sorted返回一个排序好的list，因为list中的元素是一个个的tuple，key设定按照tuple中第几个元素排序
#         # groupby把迭代器中相邻的重复元素挑出来放在一起,key设定按照tuple中第几个元素为关键字来挑选重复元素
#         # 下面的循环中groupby返回的key是intermediate_key，而group是个list，是1个或多个
#         # 有着相同intermediate_key的(intermediate_key, intermediate_value)
#         groups = {}
#         for key, group in itertools.groupby(sorted(intermediate, key=lambda im: im[0]), key=lambda x: x[0]):
#             groups[key] = [y for x, y in group]
#         # groups是一个字典，其key为上面说到的intermediate_key，value为所有对应intermediate_key的intermediate_value
#         # 组成的一个列表
#         return [reducer(intermediate_key, groups[intermediate_key]) for intermediate_key in groups]
