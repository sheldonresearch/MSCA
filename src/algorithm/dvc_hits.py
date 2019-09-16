# -*- coding: utf-8 -*-
# 专为DetectVC算法使用的修改过的HITS算法


from src.util.map_reduce import MapReduce

# from src.util import MapReduce  # 这里等MapReduce是我直接拷贝之前WWWJ期刊论文源代码里的

class HITSMapReduce:

    def __init__(self, nodes, edge, seed):
        self.max_iterations = 100  # 最大迭代次数
        self.min_delta = 0.0001  # 确定迭代是否结束的参数

        # graph表示整个网络图。是字典类型。
        # graph[i][authority][0] 存放第i网页的authority值
        # graph[i][authority][1] 存放第i网页的入链网页，是一个列表
        # graph[i][hub][0] 存放第i网页的hub值
        # graph[i][hub][1] 存放第i网页的出链网页，是一个列表
        self.graph = {}
        for node in nodes:
            if node in seed:
                self.graph[node] = {"authority": [0, []], "hub": [1, []]}
            else:
                self.graph[node] = {"authority": [0, []], "hub": [0, []]}

        for itm in edge:
            source = itm[0]
            destination = itm[1]
            self.graph[source]['hub'][1].append(destination)
            self.graph[destination]['authority'][1].append(source)

        self.seed = seed

    @staticmethod
    def normalize(ah_list):
        """
        标准化
        :param ah_list: 一个列表，其元素为(网页名，数值)
        :return: 返回一个标准化的列表，其元素为(网页名，标准化的数值)
        """
        # norm = 0
        # for ah in ah_list:
        #     norm += pow(ah[1], 2)
        #
        # norm = sqrt(norm)
        # return [(ah[0], ah[1] / norm) for ah in ah_list]
        norm = 0
        for ah in ah_list:
            norm += ah[1]
        if norm == 0:
            return [(ah[0], ah[1]) for ah in ah_list]
        return [(ah[0], ah[1] / norm) for ah in ah_list]

    def hits_authority_mapper(self, input_key, input_value):
        """
        用于计算每个页面能获得的hub值，这个hub值将传递给页面的authority值
        :param input_key: 网页名，如 A
        :param input_value: self.graph[input_key]，即这个网页的相关信息，包含两个字典，{a...}和{h...}
        :return: [(网页名, 0.0), (出链网页1, A的hub值), (出链网页2, A的hub值)...]
        """
        return [(input_key, 0.0)] + \
               [(out_link, input_value["hub"][0]) for out_link in input_value["hub"][1]]

    def hits_hub_mapper(self, input_key, input_value):
        """
        用于计算每个页面能获得的authority值，这个authority值将传递给页面的hub值
        :param input_key: 网页名，如 A
        :param input_value: self.graph[input_key]，即这个网页的相关信息，包含两个字典，{a...}和{h...}
        :return: [(网页名, 0.0), (入链网页1, A的authority值), (入链网页2, A的authority值)...]
        """
        return [(input_key, 0.0)] + \
               [(in_link, input_value["authority"][0]) for in_link in input_value["authority"][1]]

    def hits_reducer(self, intermediate_key, intermediate_value_list):
        """
        统计每个网页获得的authority或hub值
        :param intermediate_key: 网页名，如 A
        :param intermediate_value_list: A所有获得的authority值或hub值的列表:[0.0,获得的值,获得的值...]
        :return: (网页名，计算所得的authority值或hub值)
        """
        return intermediate_key, sum(intermediate_value_list)

    def hits(self):
        """
        计算authority值与hub值，各需要调用一次mapreduce模块
        :return: self.graph，其中的 authority值与hub值 已经计算好
        """
        iteration = 1  # 迭代次数
        change = 1  # 记录每轮迭代后的PR值变化情况，初始值为1保证至少有一次迭代
        while change > self.min_delta:
            print("Iteration: " + str(iteration))

            # 计算每个页面的authority值并标准化
            # new_authority为一个列表，元素为:(网页名，此轮迭代所得的authority值)
            new_authority = HITSMapReduce.normalize(
                MapReduce.map_reduce(self.graph, self.hits_authority_mapper, self.hits_reducer))

            # 计算每个页面的hub值并标准化
            # new_hub为一个列表，元素为:(网页名，此轮迭代所得的hub值)
            new_hub = HITSMapReduce.normalize(
                MapReduce.map_reduce(self.graph, self.hits_hub_mapper, self.hits_reducer))

            # 计算此轮 authority值+hub值 的变化情况
            change = sum(
                [abs(new_authority[i][1] - self.graph[new_authority[i][0]]["authority"][0]) for i in range(len(self.graph))])
            change += sum(
                [abs(new_hub[i][1] - self.graph[new_hub[i][0]]["hub"][0]) for i in range(len(self.graph))])
            print("Change: " + str(change))

            # 更新authority值与hub值
            for i in range(len(self.graph)):
                self.graph[new_authority[i][0]]["authority"][0] = new_authority[i][1]
                self.graph[new_hub[i][0]]["hub"][0] = new_hub[i][1]

            # 对于种子节点赋予其最初的值
            for id in self.seed:
                self.graph[id]["hub"][0] = 1.0
            iteration += 1
            if iteration >= 50:
                break
        return self.graph


if __name__ == '__main__':

    nodes = ["A", "B", "C", "D", "E"]
    edge = [("A", "C"), ("A", "D"), ("B", "D"), ("C", "E"), ("D", "E"), ("B", "E"), ("E", "A")]

    h = HITSMapReduce(nodes, edge)
    hits_result = h.hits()

    print("The final iteration result is")
    for key, value in hits_result.items():
        print(key + " authority: ", value["authority"][0], " hub: ", value["hub"][0])

    max_authority_page = max(hits_result.items(), key=lambda x: x[1]["authority"][0])
    max_hub_page = max(hits_result.items(), key=lambda x: x[1]["hub"][0])
    print("The best authority page: ", (max_authority_page[0], max_authority_page[1]["authority"][0]))
    print("The best hub page: ", (max_hub_page[0], max_hub_page[1]["hub"][0]))

    # ('A authority: ', 7.060561487452561e-10, ' hub: ', 0.408267180858587)
    # ('C authority: ', 0.2113248654051872, ' hub: ', 0.40823884510260666)
    # ('B authority: ', 0.0, ' hub: ', 0.7071067809972986)
    # ('E authority: ', 0.7886751345948128, ' hub: ', 8.647386468588119e-10)
    # ('D authority: ', 0.5773502691896258, ' hub: ', 0.40823884510260666)
    # ('The best authority page: ', ('E', 0.7886751345948128))
    # ('The best hub page: ', ('B', 0.7071067809972986))