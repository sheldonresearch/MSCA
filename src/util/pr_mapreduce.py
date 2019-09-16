# -*- coding: utf-8 -*-

from src.util.map_reduce import MapReduce


# from src.util import MapReduce  # 这里等MapReduce是我直接拷贝之前WWWJ期刊论文源代码里的

class PRMapReduce:
    __doc__ = '''计算PR值'''

    def __init__(self, nodes, edge):
        self.damping_factor = 0.85  # 阻尼系数,即α
        self.max_iterations = 100  # 最大迭代次数
        self.min_delta = 0.00001  # 确定迭代是否结束的参数,即ϵ
        self.num_of_pages = len(nodes)  # 总网页数

        print("16注意啦啦啦啦！！！！！")
        import operator
        print('16 ', list(edge.keys()))
        print('16 ', nodes)
        print('16 ', operator.eq(list(edge.keys()), nodes))

        # graph表示整个网络图。是字典类型。
        # graph[i][0] 存放第i网页的PR值
        # graph[i][1] 存放第i网页的出链数量
        # graph[i][2] 存放第i网页的出链网页，是一个列表
        self.graph = {}
        for node in nodes:
            self.graph[node] = [1.0 / self.num_of_pages, len(edge[node]), edge[node]]
        print("30 ok!")
        # print(self.graph['2977232342'])

    def ip_mapper(self, input_key, input_value):
        """
        看一个网页是否有出链，返回值中的 1 没有什么物理含义，只是为了在
        map_reduce中的groups字典的key只有1，对应的value为所有的悬挂网页
        的PR值
        :param input_key: 网页名，如 A
        :param input_value: self.graph[input_key]
        :return: 如果没有出链，即悬挂网页，那么就返回[(1,这个网页的PR值)]；否则就返回[]
        """
        if input_value[1] == 0:
            return [(1, input_value[0])]
        else:
            return []

    def ip_reducer(self, input_key, input_value_list):
        """
        计算所有悬挂网页的PR值之和
        :param input_key: 根据ip_mapper的返回值来看，这个input_key就是:1
        :param input_value_list: 所有悬挂网页的PR值
        :return: 所有悬挂网页的PR值之和
        """
        return sum(input_value_list)

    def pr_mapper(self, input_key, input_value):
        """
        mapper方法
        :param input_key: 网页名，如 A
        :param input_value: self.graph[input_key]，即这个网页的相关信息
        :return: [(网页名, 0.0), (出链网页1, 出链网页1分得的PR值), (出链网页2, 出链网页2分得的PR值)...]
        """
        return [(input_key, 0.0)] + [(out_link, input_value[0] / input_value[1]) for out_link in input_value[2]]

    def pr_reducer_inter(self, intermediate_key, intermediate_value_list, dp):
        """
        reducer方法
        :param intermediate_key: 网页名，如 A
        :param intermediate_value_list: A所有分得的PR值的列表:[0.0,分得的PR值,分得的PR值...]
        :param dp: 所有悬挂网页的PR值之和
        :return: (网页名，计算所得的PR值)
        """
        return (intermediate_key,
                self.damping_factor * sum(intermediate_value_list) +
                self.damping_factor * dp / self.num_of_pages +
                (1.0 - self.damping_factor) / self.num_of_pages)

    def page_rank(self):
        """
        计算PR值，每次迭代都需要两次调用MapReduce。一次是计算悬挂网页PR值之和，一次
        是计算所有网页的PR值
        :return: self.graph，其中的PR值已经计算好
        """
        iteration = 1  # 迭代次数
        change = 2  # 记录每轮迭代后的PR值变化情况，初始值为2保证至少有一次迭代
        while change > self.min_delta:
            print("Iteration: " + str(iteration))

            # 因为可能存在悬挂网页，所以才有下面这个dangling_list
            # dangling_list存放的是[所有悬挂网页的PR值之和]
            # dp表示所有悬挂网页的PR值之和
            dangling_list = MapReduce.map_reduce(self.graph, self.ip_mapper, self.ip_reducer)
            if dangling_list:
                dp = dangling_list[0]
            else:
                dp = 0

            # 因为MapReduce.map_reduce中要求的reducer只能有两个参数，而我们
            # 需要传3个参数（多了一个所有悬挂网页的PR值之和,即dp），所以采用
            # 下面的lambda表达式来达到目的
            # new_pr为一个列表，元素为:(网页名，计算所得的PR值)
            new_pr = MapReduce.map_reduce(self.graph, self.pr_mapper, lambda x, y: self.pr_reducer_inter(x, y, dp))

            # 计算此轮PR值的变化情况
            change = sum([abs(new_pr[i][1] - self.graph[new_pr[i][0]][0]) for i in range(self.num_of_pages)])
            # print("Change: " + str(change))

            # 更新PR值
            for i in range(self.num_of_pages):
                self.graph[new_pr[i][0]][0] = new_pr[i][1]
            iteration += 1

            # print(self.graph)
        return self.graph


if __name__ == '__main__':

    node = ["A", "B", "C", "D", "E"]
    edge = {'A': ['B', 'C', 'D'],
            'B': ['D', 'E'],
            'C': ['E'],
            'D': ['E'],
            'E': []}
    pr = PRMapReduce(node, edge)
    page_ranks = pr.page_rank()

    print("The final page rank is----")
    for key, value in page_ranks.items():
        print(key + " : ", value[0])
