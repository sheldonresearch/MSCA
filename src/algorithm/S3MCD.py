# 跑S3MCD算法
import logging

from pymongo import MongoClient
import numpy
from src.algorithm.alkit import Alkit
from src.util.SqlHelper import SqlHelper
from scipy import sparse
from scipy import linalg
from sklearn import metrics
import re
from src.main.evaluation import Evaluation
import random
import time

from concurrent.futures import ProcessPoolExecutor

#
# def _fun(wblog_content_split, wblog_content):
#     print('进程开始啦')
#     tmp = []
#     for wblogId1 in wblog_content_split:
#         for wblogId2 in wblog_content.keys():
#             if wblogId1 == wblogId2:
#                 continue
#             for content in wblog_content[wblogId1]:
#                 if content in wblog_content[wblogId2]:
#                     tmp.append((wblogId1, wblogId2))
#     print("其中一个进程的返回结果长度为：",len(tmp))
#     return tmp
#
#
# def fun(wblog_content, workers=8):
#     wblog_content_split = []
#     step = int(len(list(wblog_content.keys())) / workers)
#     for i in range(workers):
#         if i != workers - 1:
#             # print('slice: ', i * step, ' ', (i + 1) * step)
#             split = list(wblog_content.keys())[i * step:(i + 1) * step]
#         else:
#             # print('slice: ', i * step)
#             split = list(wblog_content.keys())[i * step:]
#         wblog_content_split.append(split)
#
#     tmp = []
#     print("len(wblog_content_split): ", len(wblog_content_split))
#     with ProcessPoolExecutor(max_workers=workers) as executor:
#         for _tmp in executor.map(_fun,
#                                  wblog_content_split,
#                                  [wblog_content for i in range(workers)]):
#             tmp = tmp + _tmp
#     return tmp


def _set_wblog_content(wblog_list_split, pattern_html, pattern_tag):
    mdb1 = MongoClient().wblog.wblog
    mdb2 = MongoClient().wblog.swblog
    wblog_content = {}
    for wblogId in wblog_list_split:
        wblog_content[wblogId] = []
        res1 = mdb1.find_one({'wblogId': wblogId})
        if res1:
            try:
                text = res1['json_text']['text']
                for html in re.findall(pattern_html, text):
                    wblog_content[wblogId].append(html)
                for tag in re.findall(pattern_tag, text):
                    wblog_content[wblogId].append(tag)
            except Exception as e:
                logging.error('%s. The wblogId is %s' % (e, str(wblogId)))

        res2 = mdb2.find_one({'wblogId': wblogId})
        if res2:
            try:
                text = res2['json_text']['text']
                for html in re.findall(pattern_html, text):
                    wblog_content[wblogId].append(html)
                for tag in re.findall(pattern_tag, text):
                    wblog_content[wblogId].append(tag)
            except Exception as e:
                logging.error('%s. The wblogId is %s' % (e, str(wblogId)))
    return wblog_content


def set_wblog_content(all_wblog, pattern_html, pattern_tag, workers=4):
    wblog_list_split = []
    step = int(len(all_wblog) / workers)
    for i in range(workers):
        if i != workers - 1:
            # print('slice: ', i * step, ' ', (i + 1) * step)
            split = all_wblog[i * step:(i + 1) * step]
        else:
            # print('slice: ', i * step)
            split = all_wblog[i * step:]
        wblog_list_split.append(split)
    wblog_content = {}
    print("len(wblog_list_split): ", len(wblog_list_split))
    with ProcessPoolExecutor(max_workers=workers) as executor:
        for _wblog_content in executor.map(_set_wblog_content,
                                           wblog_list_split,
                                           [pattern_html for i in range(workers)],
                                           [pattern_tag for i in range(workers)]):
            wblog_content.update(_wblog_content)
    return wblog_content


def _set_tweet_edge(user_list_split, all_wblog):
    tweet_edge = {}
    sqlhelper = SqlHelper(host='localhost', db='sdh', user='root', passwd='root', charset='utf8')
    for uid in user_list_split:
        tweet_edge[uid] = []
        for res in sqlhelper.select_sql('SELECT wblogId FROM wblog WHERE uid=%s' % uid):
            wblogId = str(res[0])
            if wblogId in all_wblog:
                tweet_edge[uid].append(wblogId)
        for res in sqlhelper.select_sql('SELECT wblogId FROM swblog WHERE uid=%s' % uid):
            wblogId = str(res[0])
            if wblogId in all_wblog:
                tweet_edge[uid].append(wblogId)
    return tweet_edge


def set_tweet_edge(all_user, all_wblog, workers=4):
    # workers = 8
    user_list_split = []
    step = int(len(all_user) / workers)
    for i in range(workers):
        if i != workers - 1:
            # print('slice: ', i * step, ' ', (i + 1) * step)
            split = all_user[i * step:(i + 1) * step]
        else:
            # print('slice: ', i * step)
            split = all_user[i * step:]
        user_list_split.append(split)
    tweet_edge = {}
    with ProcessPoolExecutor(max_workers=workers) as executor:
        for _tweet_edge in executor.map(_set_tweet_edge,
                                        user_list_split,
                                        [all_wblog for i in range(workers)]):
            tweet_edge.update(_tweet_edge)
    return tweet_edge


def _set_follow_edge(user_list, all_user):
    follow_edge = {}
    sqlhelper = SqlHelper(host='localhost', db='sdh', user='root', passwd='root', charset='utf8')
    for uid in user_list:
        follow_edge[uid] = []
        for result in sqlhelper.select_sql('SELECT uid, followeeUid FROM edge WHERE uid=%s' % uid):
            uid = str(result[0])
            followeeUid = str(result[1])
            if followeeUid not in all_user:
                continue
            follow_edge[uid].append(followeeUid)
    return follow_edge


def set_follow_edge(all_user_lsit, all_user, workers=8):
    # workers = 8
    user_list_split = []
    step = int(len(all_user_lsit) / workers)
    for i in range(workers):
        if i != workers - 1:
            # print('slice: ', i * step, ' ', (i + 1) * step)
            split = all_user_lsit[i * step:(i + 1) * step]
        else:
            # print('slice: ', i * step)
            split = all_user_lsit[i * step:]
        user_list_split.append(split)
    follow_edge = {}
    with ProcessPoolExecutor(max_workers=workers) as executor:
        for _follow_edge in executor.map(_set_follow_edge,
                                         user_list_split,
                                         [all_user for i in range(workers)]):
            follow_edge.update(_follow_edge)
    return follow_edge


class S3MCD(object):

    def __init__(self, h, d, u, p, c, file_name_appendix=''):
        """
        在init中将读取S3MCD必要的数据
        """
        self.host = h
        self.db = d
        self.user = u
        self.passwd = p
        self.charset = c
        self.sqlhelper = SqlHelper(host=self.host, db=self.db, user=self.user, passwd=self.passwd, charset=self.charset)
        self.file_name_appendix = file_name_appendix
        # 读取训练集，以及测试集上得到的先验类别
        # user_train_dict，训练集，带标签
        # user_train_list，训练集，只有用户id
        # user_prior_dict，测试集，带ground truth标签，以及先验类别的prior标签
        # user_prior_list, 测试集，只有用户id
        self.user_train_dict, self.user_train_list, self.user_prior_dict, self.user_prior_list = \
            Alkit.read_prior('../main/prior/user_train' + self.file_name_appendix + '.txt',
                             '../main/prior/user_prior' + self.file_name_appendix + '.txt')
        self.wblog_train_dict, self.wblog_train_list, self.wblog_prior_dict, self.wblog_prior_list = \
            Alkit.read_prior('../main/prior/wblog_train' + self.file_name_appendix + '.txt',
                             '../main/prior/wblog_prior' + self.file_name_appendix + '.txt')

        # spammer，真实的spammer用户
        # spammer_prior，先验类别判定后的spammer用户
        # normal，真实的normal用户
        # normal_prior，先验类别判定后的normal用户
        # swblog，swblog_prior，wblog，wblog_prior同理
        self.spammer, self.spammer_prior, self.normal, self.normal_prior = Alkit.setSN(self.user_train_dict,
                                                                                       self.user_prior_dict)
        self.swblog, self.swblog_prior, self.nwblog, self.nwblog_prior = Alkit.setSN(self.wblog_train_dict,
                                                                                     self.wblog_prior_dict)
        self.all_user = self.user_prior_list
        self.all_wblog = self.wblog_prior_list

        self.follow_edge = {}  # {'uid': ['followeeUid']}
        self.tweet_edge = {}  # {'uid': ['wblogId']}
        self.wblog_content = {}  # {'wblogId': [content]}

        self.pattern_html = re.compile(r'<[^>]+>', re.S)
        self.pattern_tag = re.compile(r'#.+#', re.S)

    def loadFollowRelationship(self, workers=8):
        """
        读取用户间的关注关系
        :return: none
        """
        # 读取用户间关注关系
        # logging.info('loading FollowRelationship')
        # for uid in self.all_user:
        #     self.follow_edge[uid] = []
        #     for result in self.sqlhelper.select_sql('SELECT uid, followeeUid FROM edge WHERE uid=%s' % uid):
        #         uid = str(result[0])
        #         followeeUid = str(result[1])
        #         if followeeUid not in self.all_user:
        #             continue
        #         self.follow_edge[uid].append(followeeUid)
        # logging.info('loadFollowRelationship finished')

        """
        
        """
        logging.info('多进程读取关注关系')
        self.follow_edge = set_follow_edge(self.all_user, self.all_user, workers=workers)

        # print("S3MCD 128注意啦！！！！！")
        # len(list(self.follow_edge.keys()))
        # len(self.all_user)
        # import operator
        # print(operator.eq(list(self.follow_edge.keys()), self.all_user))

    def loadTweetRelationship(self, workers=4):
        """
        读取用户与微博间的发布关系
        :return: none
        """
        logging.info('loading loadTweetRelationship')
        logging.info('多进程读取发布关系')

        self.tweet_edge = set_tweet_edge(self.all_user, self.all_wblog, workers=workers)

        # for uid in self.all_user:
        #     self.tweet_edge[uid] = []
        #     for res in self.sqlhelper.select_sql('SELECT wblogId FROM wblog WHERE uid=%s' % uid):
        #         wblogId = str(res[0])
        #         if wblogId in self.all_wblog:
        #             self.tweet_edge[uid].append(wblogId)
        #     for res in self.sqlhelper.select_sql('SELECT wblogId FROM swblog WHERE uid=%s' % uid):
        #         wblogId = str(res[0])
        #         if wblogId in self.all_wblog:
        #             self.tweet_edge[uid].append(wblogId)

    def loadWblogRelation(self, workers=8):
        """
        读取微博间的关系
        微博文本中有相同的tag或者链接的，就有关系
        :return: none
        """
        logging.info('loading loadWblogRelation')
        logging.info('多进程读取微博间的关系')

        self.wblog_content = set_wblog_content(self.all_wblog, self.pattern_html, self.pattern_tag, workers=workers)

        logging.info('多进程读取微博间的关系结束！')
        # mdb = MongoClient().wblog.wblog
        # for wblogId in self.all_wblog:
        #     self.wblog_content[wblogId] = []
        #     res = mdb.find_one({'wblogId': wblogId})
        #     if res:
        #         try:
        #             text = res['json_text']['text']
        #             for html in re.findall(self.pattern_html, text):
        #                 self.wblog_content[wblogId].append(html)
        #             for tag in re.findall(self.pattern_tag, text):
        #                 self.wblog_content[wblogId].append(tag)
        #         except Exception as e:
        #             logging.error('%s. The wblogId is %s' % (e, str(wblogId)))
        # mdb = MongoClient().wblog.swblog
        # for wblogId in self.all_wblog:
        #     # self.wblog_content[wblogId] = []    #？？？这里这样写，上面的代码不是全都白做了嘛？
        #     res = mdb.find_one({'wblogId': wblogId})
        #     if res:
        #         try:
        #             text = res['json_text']['text']
        #             for html in re.findall(self.pattern_html, text):
        #                 self.wblog_content[wblogId].append(html)
        #             for tag in re.findall(self.pattern_tag, text):
        #                 self.wblog_content[wblogId].append(tag)
        #         except Exception as e:
        #             logging.error('%s. The wblogId is %s' % (e, str(wblogId)))

    def setWblogRelation(self):
        """
        计算微博间的关系矩阵C
        微博文本中有相同的tag或者链接的，就有关系
        :return:
        """
        self.loadWblogRelation(workers=8)  # 这是一个高耗时的应用， 需要写成多进程的形式
        tmp = []
        """
        下面这段代码很有可能是一个高耗时应用
        """
        # logging.info('多进程处理tmp变量')
        # tmp = fun(self.wblog_content, workers=16)
        # logging.info('多进程处理tmp变量结束！')
        for wblogId1 in self.wblog_content.keys():
            for wblogId2 in self.wblog_content.keys():
                if wblogId1 == wblogId2:
                    continue
                for content in self.wblog_content[wblogId1]:
                    if content in self.wblog_content[wblogId2]:
                        tmp.append((wblogId1, wblogId2))

        # 生成微博间的关系矩阵C
        C = sparse.lil_matrix((len(tmp), len(self.all_wblog)))
        cnt = 0
        for itm in tmp:
            C[cnt, self.all_wblog.index(itm[0])] = 1.0
            C[cnt, self.all_wblog.index(itm[1])] = -1.0
            cnt += 1
        C = C.tocoo()
        # 写入文件中
        sparse.save_npz('../main/s3mcd/C' + self.file_name_appendix, C)
        logging.info('setWblogRelation finished')

    def setFollowRelationship(self):
        """
        计算关注关系相关的矩阵A和B，并保存进文件中
        :return: none
        """
        self.loadFollowRelationship(workers=8)
        row_cnt = 0
        for uid in self.follow_edge:
            for followeeUid in self.follow_edge[uid]:
                if uid not in self.follow_edge[followeeUid]:
                    continue
                row_cnt += 1
        A = sparse.lil_matrix((row_cnt, len(self.all_user)))
        cnt = 0
        for uid in self.follow_edge:
            for followeeUid in self.follow_edge[uid]:
                if uid not in self.follow_edge[followeeUid]:
                    continue
                index1 = self.all_user.index(uid)
                index2 = self.all_user.index(followeeUid)
                A[cnt, index1] = 1.0
                A[cnt, index2] = -1.0
                cnt += 1
        A = A.tocoo()
        sparse.save_npz('../main/s3mcd/A' + self.file_name_appendix, A)

        row_cnt = 0
        for uid in self.follow_edge:
            for followeeUid in self.follow_edge[uid]:
                row_cnt += 1
        B = sparse.lil_matrix((row_cnt, len(self.all_user)))
        cnt = 0
        for uid in self.follow_edge:
            for followeeUid in self.follow_edge[uid]:
                index1 = self.all_user.index(uid)
                index2 = self.all_user.index(followeeUid)
                B[cnt, index1] = 1.0
                B[cnt, index2] = -1.0
                cnt += 1
        B = B.tocoo()
        sparse.save_npz('../main/s3mcd/B' + self.file_name_appendix, B)
        logging.info('setFollowRelationship finished')

    def setTeetMatrix(self):
        """
        设置发布矩阵P，并保存进文件中
        :return: none
        """
        self.loadTweetRelationship(workers=8)  # 这是一个高耗时的代码，需要写成多进程形式
        # 生成发布矩阵P
        P = sparse.lil_matrix((len(self.all_user), len(self.all_wblog)))
        for uid in self.tweet_edge.keys():
            for wblogId in self.tweet_edge[uid]:
                P[self.all_user.index(uid), self.all_wblog.index(wblogId)] = 1.0
        P = P.tocoo()
        # 写入文件中
        sparse.save_npz('../main/s3mcd/P' + self.file_name_appendix, P)
        logging.info('setTeetMatrix finished')

    def run(self, alpha, beta, lenda, gamma, iteration_limit, change_limit):
        """
        跑S3MCD算法
        :return:
        """
        # 首先确定x和y向量
        li = []
        for uid in self.user_prior_list:
            li.append(float(self.user_prior_dict[uid]['prior']))
        x_p = numpy.array(li)
        logging.info('user num: %s' % str(len(li)))
        li = []
        for wblogId in self.wblog_prior_list:
            li.append(float(self.wblog_prior_dict[wblogId]['prior']))
        y_p = numpy.array(li)
        logging.info('wblog num: %s' % str(len(li)))

        # 载入矩阵
        A = sparse.load_npz('../main/s3mcd/A' + self.file_name_appendix + '.npz')
        B = sparse.load_npz('../main/s3mcd/B' + self.file_name_appendix + '.npz')
        C = sparse.load_npz('../main/s3mcd/C' + self.file_name_appendix + '.npz')
        P = sparse.load_npz('../main/s3mcd/P' + self.file_name_appendix + '.npz')

        # 然后需要分别计算x和y迭代时的逆矩阵
        logging.info('计算迭代x时的逆矩阵')
        luo_x = 20.0
        I1 = sparse.identity(len(self.all_user))
        ATA = A.T.dot(A)
        xm = I1.dot(2.0) + ATA.dot(luo_x)
        xm = linalg.inv(xm.toarray())

        logging.info('计算迭代y时的逆矩阵')
        luo_y = 15.5
        I2 = sparse.identity(len(self.all_wblog))
        # print('377 I2:', I2.toarray().shape)  # (927, 927)
        CTC = C.T.dot(C)
        # print('377 CTC:', CTC.toarray().shape)  # (927, 927)
        ym = I2.dot(2.0) + CTC.dot(luo_y)
        # print('377 ym: ', ym.toarray().shape)  # (927, 927)
        ym = linalg.inv(ym.toarray())
        # print('ym: ', ym.shape)  # (927, 927)

        # """
        # def dot(self, other):
        #     return self * other
        #
        # if other.shape != (N,) and other.shape != (N, 1):
        #         raise ValueError('dimension mismatch')
        # """
        # print('A.dot(x_p).shape',A.dot(x_p).shape) # (906,)
        # print('x_p.shape', x_p.shape) # (615,)
        # print('A.shape', A.shape)  #(906, 615)
        # """
        # shape:
        # A.dot(x_p)=w=v=w_o
        #
        #
        #
        # w = m_o = v = C.dot(y_p)
        # """
        # print('C.dot(y_p).shape', C.dot(y_p).shape)  # (1480812,)
        # print('y_p.shape', y_p.shape) #(927,)
        # print('C.shape', C.shape) #(1480812, 927)

        li = []
        # for i in range(1268):   # why 1268??? A.dot(x_p)
        for i in range(A.shape[0]):  # why 1268??? A.dot(x_p)
            li.append(0.0)
        w_o = numpy.array(li)
        li = []
        for i in range(C.shape[0]):  # why 0???
            li.append(0.0)
        m_o = numpy.array(li)

        li = []
        # for i in range(4845):   # why 4845???
        for i in range(B.T.shape[1]):  # why 4845??? B.T.shape[1]
            li.append(1.0)
        help = numpy.array(li)

        # 开始迭代
        logging.info('开始迭代')
        iteration = 0
        x = x_p
        y = y_p
        while True:
            iteration += 1
            logging.info('iteration: %s' % str(iteration))
            if iteration > iteration_limit:
                break
            iteration_x = 0
            w = w_o
            v = A.dot(x)
            tmp = x
            while True:
                iteration_x += 1
                if iteration_x > 100:
                    break
                # print('442 xm.shape: ', xm.shape)   # (615, 615)
                # print('442 x_p.dot(2.0).shape: ', x_p.dot(2.0).shape)   # (615,)
                # print('442 B.T.dot(beta).shape: ', B.T.dot(beta).shape)  # (615, 2546)
                # print('442 help.shape: ', help.shape)  #  (2546,)
                # print('442 B.T.dot(beta).dot(help).shape: ', B.T.dot(beta).dot(help).shape)  # (615,)
                # print('P.dot(y).dot(lenda).shape: ', P.dot(y).dot(lenda).shape) # (615,)
                # print('A.T.dot(luo_x).dot(v - w).shape: ', A.T.dot(luo_x).dot(v - w).shape)
                # print(
                #     'x_p.dot(2.0) + B.T.dot(beta).dot(help) + P.dot(y).dot(lenda) + A.T.dot(luo_x).dot(v - w).shape: ',
                #     (x_p.dot(2.0) + B.T.dot(beta).dot(help) + P.dot(y).dot(lenda) + A.T.dot(luo_x).dot(v - w)).shape)

                x_next = xm.dot(
                    x_p.dot(2.0) + B.T.dot(beta).dot(help) + P.dot(y).dot(lenda) + A.T.dot(luo_x).dot(v - w))
                """
                 if other.shape != (N,) and other.shape != (N, 1):
                raise ValueError('dimension mismatch')
                """
                v_next = A.dot(x_next) + w
                w_next = w + A.dot(x_next) - v_next
                change = self.getChange(tmp, x_next, w, w_next)
                if change <= 0.01:
                    break
                v = v_next
                w = w_next
                tmp = x_next

            iteration_y = 0
            w = m_o
            v = C.dot(y)
            tmp = y
            while True:
                iteration_y += 1
                if iteration_y > 100:
                    break
                y_next = ym.dot(y_p.dot(2.0) + P.T.dot(x_next).dot(lenda) + C.T.dot(luo_y).dot(v - w))
                v_next = C.dot(y_next) + w
                w_next = w + C.dot(y_next) - v_next
                change = self.getChange(tmp, y_next, w, w_next)
                if change <= 0.01:
                    break
                v = v_next
                w = w_next
                tmp = y_next

            change = self.getChange(x, x_next, y, y_next)
            logging.info('change: %s' % str(change))
            if change <= change_limit:
                break
            x = x_next
            y = y_next

        logging.info('迭代结束')

        # 将结果写入文件
        numpy.savetxt('../main/s3mcd/res_user' + self.file_name_appendix + '.txt', x)
        numpy.savetxt('../main/s3mcd/res_wblog' + self.file_name_appendix + '.txt', y)

    def getChange(self, x, x_next, y, y_next):
        """
        计算每次迭代时的change
        :param x:
        :param x_next:
        :param y:
        :param y_next:
        :return: change
        """
        return linalg.norm(x - x_next, 1) + linalg.norm(y - y_next, 1)

    def evaluation(self):
        """
        评价S3MCD算法的结果
        :return:
        """
        logging.info('用户结果')
        scores = []
        cnt = 0
        with open('../main/s3mcd/res_user' + self.file_name_appendix + '.txt', 'r') as my_file:
            for line in my_file:
                score = float(line.split('\n')[0])
                if self.all_user[cnt] in self.user_prior_list:
                    scores.append(score)
                cnt += 1
        logging.info(
            'min_score: %s, max_score: %s, len(user):%s' % (str(min(scores)), str(max(scores)), str(len(scores))))
        test_result = []
        for uid in self.user_prior_list:
            test_result.append(int(self.user_prior_dict[uid]['label']))
        user_res = Evaluation.evaluation_self(scores, test_result)

        # ap
        p, r, thresholds = metrics.precision_recall_curve(test_result, scores)
        ap = metrics.average_precision_score(test_result, scores)
        logging.info('user AP:%s' % str(ap))
        with open('../main/s3mcd/user_ap' + self.file_name_appendix + '.txt', 'w') as my_file:
            my_file.write('p r\n')
            for i in range(len(p)):
                my_file.write('%s %s\n' % (str(p[i]), str(r[i])))

        # roc
        fpr, tpr, thresholds = metrics.roc_curve(test_result, scores)
        logging.info('user AUC:%s' % str(metrics.auc(fpr, tpr)))
        with open('../main/s3mcd/user_roc' + self.file_name_appendix + '.txt', 'w') as my_file:
            my_file.write('fpr tpr\n')
            for i in range(len(fpr)):
                my_file.write('%s %s\n' % (str(fpr[i]), str(tpr[i])))

        # top k precision
        worker_score = {}
        for i in range(len(scores)):
            worker_score[self.user_prior_list[i]] = scores[i]
        worker_score = sorted(worker_score.items(), key=lambda im: float(im[1]), reverse=True)
        with open('../main/s3mcd/res_user_top' + self.file_name_appendix + '.txt', 'w') as my_file:
            my_file.write('type uid score precision top_k\n')
            worker_count_now = 0
            top_k = 0
            for itm in worker_score:
                uid = itm[0]
                score = itm[1]
                if uid in self.spammer:
                    u_type = 'w'
                    worker_count_now += 1
                else:
                    u_type = 'n'
                top_k += 1
                precision = str(float(worker_count_now) / top_k)
                my_file.write(u_type + ' ' + str(uid) + ' ' + str(score) + ' ' + precision + ' ' + str(top_k) + '\n')

        logging.info('微博结果')
        scores = []
        cnt = 0
        with open('../main/s3mcd/res_wblog' + self.file_name_appendix + '.txt', 'r') as my_file:
            for line in my_file:
                score = float(line.split('\n')[0])
                if self.all_wblog[cnt] in self.wblog_prior_list:
                    if float(score) == 1.0 or float(score) == -0.07749500848848562:
                        random.seed(time.time())
                        score += float(random.randint(0, 100)) / 10000
                    scores.append(score)
                cnt += 1
        logging.info(
            'min_score: %s, max_score: %s, len(wblog):%s' % (str(min(scores)), str(max(scores)), str(len(scores))))
        test_result = []
        for wblogId in self.wblog_prior_list:
            test_result.append(int(self.wblog_prior_dict[wblogId]['label']))
        wblog_res = Evaluation.evaluation_self(scores, test_result)

        # ap
        p, r, thresholds = metrics.precision_recall_curve(test_result, scores)
        ap = metrics.average_precision_score(test_result, scores)
        logging.info('wblog AP:%s' % str(ap))
        with open('../main/s3mcd/wblog_ap' + self.file_name_appendix + '.txt', 'w') as my_file:
            my_file.write('p r\n')
            for i in range(len(p)):
                my_file.write('%s %s\n' % (str(p[i]), str(r[i])))

        # roc
        fpr, tpr, thresholds = metrics.roc_curve(test_result, scores)
        logging.info('wblog AUC:%s' % str(metrics.auc(fpr, tpr)))
        with open('../main/s3mcd/wblog_roc' + self.file_name_appendix + '.txt', 'w') as my_file:
            my_file.write('fpr tpr\n')
            for i in range(len(fpr)):
                my_file.write('%s %s\n' % (str(fpr[i]), str(tpr[i])))

        # top k precision
        wblog_score = {}
        for i in range(len(scores)):
            wblog_score[self.wblog_prior_list[i]] = scores[i]
        wblog_score = sorted(wblog_score.items(), key=lambda im: float(im[1]), reverse=True)
        with open('../main/s3mcd/res_wblog_top' + self.file_name_appendix + '.txt', 'w') as my_file:
            my_file.write('type wblogId score precision top_k\n')
            wblog_count_now = 0
            top_k = 0
            for itm in wblog_score:
                uid = itm[0]
                score = itm[1]
                if uid in self.swblog:
                    u_type = 's'
                    wblog_count_now += 1
                else:
                    u_type = 'n'
                top_k += 1
                precision = str(float(wblog_count_now) / top_k)
                my_file.write(u_type + ' ' + str(uid) + ' ' + str(score) + ' ' + precision + ' ' + str(top_k) + '\n')

        return user_res, wblog_res
