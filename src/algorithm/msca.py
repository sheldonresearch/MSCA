# 跑MSCA算法
import logging

from pymongo import MongoClient
import numpy
from src.algorithm.alkit import Alkit
from src.util.SqlHelper import SqlHelper
from scipy import sparse
from src.util.pr_mapreduce import PRMapReduce
from scipy import linalg
import scipy
from sklearn import metrics
from src.main.evaluation import Evaluation
import random

logging.info('start process!')

from concurrent.futures import ProcessPoolExecutor


def _set_retweet_edge(user_list, all_wblog):
    retweet_edge = {}
    sqlhelper = SqlHelper(host='localhost', db='sdh', user='root', passwd='root', charset='utf8')
    for uid in user_list:
        retweet_edge[uid] = []
        for res in sqlhelper.select_sql('SELECT paMid, orMid FROM wblog WHERE uid=%s' % uid):
            paMid = str(res[0])
            orMid = str(res[1])
            if paMid in all_wblog:
                retweet_edge[uid].append(paMid)
            if orMid in all_wblog:
                retweet_edge[uid].append(orMid)
    return retweet_edge


def set_retweet_edge(all_user, all_wblog, workers=8):
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
    retweet_edge = {}
    with ProcessPoolExecutor(max_workers=workers) as executor:
        for _retweet_edge in executor.map(_set_retweet_edge,
                                          user_list_split, [all_wblog for i in range(workers)]):
            retweet_edge.update(_retweet_edge)
    return retweet_edge


def _set_follow_edge(user_list, all_user, spammer_prior, normal_prior):
    follow_edge = {}
    sqlhelper = SqlHelper(host='localhost', db='sdh', user='root', passwd='root', charset='utf8')
    for uid in user_list:
        follow_edge[uid] = []
        for result in sqlhelper.select_sql('SELECT uid, followeeUid FROM edge WHERE uid=%s' % uid):
            uid = str(result[0])
            followeeUid = str(result[1])
            if followeeUid not in all_user:
                continue
            if uid in spammer_prior and followeeUid in normal_prior:
                continue
            follow_edge[uid].append(followeeUid)
    return follow_edge


def set_follow_edge(all_user_lsit, all_user, spammer_prior, normal_prior, workers=8):
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
                                         [all_user for i in range(workers)],
                                         [spammer_prior for i in range(workers)],
                                         [normal_prior for i in range(workers)]):
            follow_edge.update(_follow_edge)
    return follow_edge


def _com_s(retweet_edge_key_list_partion, id1, id2, retweet_edge, retweet_cnt):
    s = 0
    for key in retweet_edge_key_list_partion:
        if len(retweet_edge[key]) == 0:
            continue
        if id1 in retweet_edge[key] and id2 in retweet_edge[key]:
            s += 1.0 / float(retweet_cnt[key])
    return s


def _com_RI(id1_id2_tuple_list_partion, retweet_cnt, retweet_cnt2, retweet_edge, workers=2):
    lines = []
    nc = 0
    for id1, id2 in id1_id2_tuple_list_partion:
        s = 0.0
        if retweet_cnt2[id1] == 0 or retweet_cnt2[id2] == 0:
            s = 0.0
        else:
            # retweet_edge_key_list_partion = []
            # retweet_edge_key_list = list(retweet_edge.keys())
            # step = int(len(retweet_edge_key_list) / workers)
            # for i in range(workers):
            #     if i != workers - 1:
            #         # print('slice: ', i * step, ' ', (i + 1) * step)
            #         split = retweet_edge_key_list[i * step:(i + 1) * step]
            #     else:
            #         # print('slice: ', i * step)
            #         split = retweet_edge_key_list[i * step:]
            #     retweet_edge_key_list_partion.append(split)
            # logging.info('任务划分结束，开启多进程调用_com_s')
            # with ProcessPoolExecutor(max_workers=workers) as executor:
            #     for _s in executor.map(_com_s,
            #                            retweet_edge_key_list_partion,
            #                            [id1 for i in range(workers)],
            #                            [id2 for i in range(workers)],
            #                            [retweet_edge for i in range(workers)],
            #                            [retweet_cnt for i in range(workers)]):
            #         s = s + _s
            """
             通过测试我们发现，嵌套多进程可能造成系统资源浪费，因此这里暂时把_com_s的workers设为1
             """
            for key in retweet_edge.keys():
                if len(retweet_edge[key]) == 0:
                    continue
                if id1 in retweet_edge[key] and id2 in retweet_edge[key]:
                    s += 1.0 / float(retweet_cnt[key])
        if s != 0.0:
            nc += 1
            lines.append('%s %s %s\n' % (id1, id2, str(s)))
            # my_file.write('%s %s %s\n' % (id1, id2, str(s)))
    return nc, lines


def compute_relation_intensity(type, target, retweet_cnt, retweet_cnt2, retweet_edge, file_name_appendix, workers=2):
    # 然后计算用户两两间的联系强度，并生成一个用户数*用户数的方阵S
    # 因为我后面会将这个方阵写入文件中，所以就不真正生成S了
    # 对于微博同理
    if type == 'wblog':
        inner_workers = int(workers * 2)
    else:
        inner_workers = workers

    logging.info("type| target length: " + type + ' ' + str(len(target)))
    id1_id2_tuple_list = []
    for i in range(len(target)):
        id1 = target[i]
        for j in range(i + 1, len(target)):
            id2 = target[j]
            id1_id2_tuple_list.append((id1, id2))

    nc = 0  # 记录方阵S中不为0的元素数

    id1_id2_tuple_list_partion = []
    step = int(len(id1_id2_tuple_list) / inner_workers)
    for i in range(inner_workers):
        if i != inner_workers - 1:
            # print('slice: ', i * step, ' ', (i + 1) * step)
            split = id1_id2_tuple_list[i * step:(i + 1) * step]
        else:
            # print('slice: ', i * step)
            split = id1_id2_tuple_list[i * step:]
        id1_id2_tuple_list_partion.append(split)
    logging.info('子任务个数：' + str(len(id1_id2_tuple_list_partion)))
    for sub in id1_id2_tuple_list_partion:
        logging.info('子任务大小为：' + str(len(sub)))
    logging.info('任务划分结束，开启多进程调用_com_RI')
    all_lines = []
    with ProcessPoolExecutor(max_workers=inner_workers) as executor:
        for _nc, lines in executor.map(_com_RI,
                                       id1_id2_tuple_list_partion,
                                       [retweet_cnt for i in range(inner_workers)],
                                       [retweet_cnt2 for i in range(inner_workers)],
                                       [retweet_edge for i in range(inner_workers)]):
            nc = nc + _nc
            all_lines.append(lines)

    logging.info("计算结束，开始写入文件" + '../main/relation_intensity/%s' % type + file_name_appendix + '.txt')
    with open('../main/relation_intensity/%s' % type + file_name_appendix + '.txt', 'w') as my_file:
        for lines in all_lines:
            for line in lines:
                my_file.write(line)
    logging.info("写入文件结束")
    # for id1, id2 in id1_id2_tuple_list:
    #     s = 0.0
    #     if retweet_cnt2[id1] == 0 or retweet_cnt2[id2] == 0:
    #         s = 0.0
    #     else:
    #         for key in retweet_edge.keys():
    #             if len(retweet_edge[key]) == 0:
    #                 continue
    #             if id1 in retweet_edge[key] and id2 in retweet_edge[key]:
    #                 s += 1.0 / float(retweet_cnt[key])
    #     if s != 0.0:
    #         nc += 1
    #         my_file.write('%s %s %s\n' % (id1, id2, str(s)))

    logging.info('%s, nc=%s' % (type, str(nc)))


class MSCA(object):

    def __init__(self, h, d, u, p, c, file_name_appendix=''):
        """
        在init中将读取msca必要的数据
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

        # self.user_train_dict, self.user_train_list, self.user_prior_dict, self.user_prior_list = \
        #     Alkit.read_prior('prior_bak/user_train.txt', 'prior_bak/user_prior.txt')
        # self.wblog_train_dict, self.wblog_train_list, self.wblog_prior_dict, self.wblog_prior_list = \
        #     Alkit.read_prior('prior_bak/wblog_train.txt', 'prior_bak/wblog_prior.txt')

        # spammer，真实的spammer用户
        # spammer_prior，先验类别判定后的spammer用户
        # normal，真实的normal用户
        # normal_prior，先验类别判定后的normal用户
        # swblog，swblog_prior，wblog，wblog_prior同理
        self.spammer, self.spammer_prior, self.normal, self.normal_prior = Alkit.setSN(self.user_train_dict,
                                                                                       self.user_prior_dict)
        self.swblog, self.swblog_prior, self.nwblog, self.nwblog_prior = Alkit.setSN(self.wblog_train_dict,
                                                                                     self.wblog_prior_dict)
        self.all_user = self.user_train_list + self.user_prior_list
        self.all_wblog = self.wblog_train_list + self.wblog_prior_list

        self.follow_edge = {}  # {'uid': ['followeeUid']}
        self.follow_cnt = {}  # {'uid': follow count}
        self.retweet_edge = {}  # {'uid': ['wblogId']}
        self.wblog_retweet_cnt = {}  # {wblogId: retweet count}
        self.user_retweet_cnt = {}  # {uid: retweet count}

    def loadFollowRelationship(self, workers=8):
        """
        读取用户间的关注关系
        :return: none
        """
        # 读取用户间关注关系
        # 注意spammer关注normal的边需要去除
        # 去除包括user_train里面的这种边 以及 user_prior里面的这种边（user_prior里面根据prior_label来确定）
        logging.info('多进程读取关注关系')
        self.follow_edge = set_follow_edge(self.all_user, self.all_user, self.spammer_prior, self.normal_prior,
                                           workers=workers)

        print("注意啦！！！！！")
        len(list(self.follow_edge.keys()))
        len(self.all_user)
        import operator
        print(operator.eq(list(self.follow_edge.keys()), self.all_user))

        """
        下面一段的注视是原来的代码，因为速度太慢， 我将其改造成了上面的多进程形式
        """
        # logging.info('loading FollowRelationship')
        # for uid in self.all_user[0:8]:
        #     self.follow_edge[uid] = []
        #     for result in self.sqlhelper.select_sql('SELECT uid, followeeUid FROM edge WHERE uid=%s' % uid):
        #         uid = str(result[0])
        #         followeeUid = str(result[1])
        #         if followeeUid not in self.all_user:
        #             continue
        #         if uid in self.spammer_prior and followeeUid in self.normal_prior:
        #             continue
        #         self.follow_edge[uid].append(followeeUid)
        # print('180 ', self.follow_edge)
        #
        # import operator
        # print(operator.eq(follow_edge,self.follow_edge))
        # print(follow_edge)
        # print(len(follow_edge))
        # print(self.follow_edge)
        # print(len(self.follow_edge))

        # 统计每个用户的关注数，方便后面的计算
        # 这里就统计这三千多个用户中的，就不统计总的粉丝数了
        for uid in self.all_user:
            self.follow_cnt[uid] = 0
        for uid in self.follow_edge.keys():
            self.follow_cnt[uid] += len(self.follow_edge[uid])

        logging.info('多进程读取关注关系处理结束！')

    def loadRetweetRelationship(self, workers=8):
        """
        读取用户与微博间的转发关系 以及 微博的转发数 和 用户的转发数
        :return: none
        """
        # 读取转发关系
        # 注意除了wblog表中三个月的微博数据外，还需要考虑spammer对于swblog的转发
        # 本来想根据提交的众包任务来确定spammer与swblog的转发关系的，但是刚发现不行，不行的原因有两点：
        # 1.mission表中没有wblogId，只有微博短id，无法匹配，好像我之前确定swblog的wblogId的时候是一条条人工记录的
        # 2.有一些水军提交任务的时候是浑水摸鱼的，可能啥都没干，也可能贴的错误的回复
        # 所以换一种方法
        # 之前爬评论的时候专门爬取了swblog的评论，就将评论了swblog的用户全部当做转发了
        logging.info('多进程读取转发关系')  # 3884个用户全部处理完大概需要30min
        self.retweet_edge = set_retweet_edge(self.all_user, self.all_wblog, workers=workers)
        """
        下面一段的注视是原来的代码，因为速度太慢， 我将其改造成了上面的多进程形式
        """
        # logging.info('non-process!')
        # uid_count = 0
        # for uid in self.all_user[0:80]:
        #     # for uid in all_user_sample:
        #     uid_count = uid_count + 1
        #     if uid_count % 500 == 0:
        #         logging.info("outerloop: {}/{}={}%".format(str(uid_count), str(len(self.all_user)),
        #                                                    str(100.0 * uid_count / len(self.all_user))))
        #     self.retweet_edge[uid] = []
        #     for res in self.sqlhelper.select_sql('SELECT paMid, orMid FROM wblog WHERE uid=%s' % uid):
        #         paMid = str(res[0])
        #         orMid = str(res[1])
        #         if paMid in self.all_wblog:
        #             self.retweet_edge[uid].append(paMid)
        #         if orMid in self.all_wblog:
        #             self.retweet_edge[uid].append(orMid)
        # import operator
        #
        # print(operator.eq(retweet_edge, self.retweet_edge))

        logging.info("retweet_edge...")
        mdb = MongoClient().comment.comment
        for wblogId in self.swblog:
            # for wblogId in sw_sample:
            for res in mdb.find({'wblogId': wblogId}):
                try:
                    uid = res['json_text']['user']['id']
                    if uid in self.retweet_edge.keys():
                        if wblogId not in self.retweet_edge[uid]:
                            self.retweet_edge[uid].append(wblogId)
                except Exception as e:
                    logging.error('%s. The wblogId is %s' % (e, str(wblogId)))

        logging.info('读取微博的转发数')
        # 读取每条微博的转发数，方便后面计算用户间的联系强度
        # print(len(self.all_wblog))
        for wblogId in self.all_wblog:
            self.wblog_retweet_cnt[wblogId] = 0
        for uid in self.retweet_edge.keys():
            for wblogId in self.retweet_edge[uid]:
                self.wblog_retweet_cnt[wblogId] += 1

        # # 下面是统计一条微博总的转发数，也即转发数会很大
        #
        # mdb1 = MongoClient().wblog.wblog
        # mdb2 = MongoClient().wblog.swblog

        # suc=0
        # fail=0
        # logging.info('测试点！')
        # for wblogId in self.all_wblog:
        #     try:
        #         wblog = mdb1.find_one({'wblogId': wblogId})
        #         self.wblog_retweet_cnt[wblogId] = int(wblog['json_text']['reposts_count'])
        #         wblog = mdb2.find_one({'wblogId': wblogId})
        #         self.wblog_retweet_cnt[wblogId] = int(wblog['json_text']['reposts_count'])
        #         suc = suc + 1
        #         print("LINE:172 | suc: ", suc, "fail: ", fail)
        #     except Exception as e:
        #         fail=fail+1
        #         # logging.error('%s. The wblogId is %s' % (e, str(wblogId)))
        # logging.error('success %s, fail %s' %(str(suc),str(fail)))

        # mdb = MongoClient().wblog.wblog
        #
        # suc = 0
        # fail = 0
        # for wblogId in self.nwblog:
        #     try:
        #         wblog = mdb.find_one({'wblogId': wblogId})
        #         self.wblog_retweet_cnt[wblogId] = int(wblog['json_text']['reposts_count'])
        #         suc = suc + 1
        #     except Exception as e:
        #         fail = fail + 1
        #         # print("LINE:187 | suc: ", suc, "fail: ", fail)
        #         logging.error('%s. The wblogId is %s' % (e, str(wblogId)))
        # logging.error('for wblogId in self.nwblog... success %s, fail %s' % (str(suc), str(fail)))

        # mdb = MongoClient().wblog.swblog
        #
        # suc = 0
        # fail = 0
        # for wblog in mdb.find():
        #     try:
        #         self.wblog_retweet_cnt[wblog['json_text']['id']] = wblog['json_text']['reposts_count']
        #         suc = suc + 1
        #     except Exception as e:
        #         fail = fail + 1
        #         # print("LINE:199 | suc: ", suc, "fail: ", fail)
        #         # logging.error('%s.' % e)
        # logging.error('or wblog in mdb.find():... success %s, fail %s' % (str(suc), str(fail)))

        logging.info('读取用户的转发数')
        # 同样的，读取每个用户的转发数，方便后面计算微博间的联系强度
        # 由于用户的转发数和微博的转发数的获取难度不同，后者在json里就有，前者没有，所以我就只统计这三个月的了
        for uid in self.all_user:
            self.user_retweet_cnt[uid] = len(self.retweet_edge[uid])
        logging.info('loadRetweetRelationship finished')

    def setRelationIntensity_new(self, type, target, workers=4):
        """
        计算用户间的联系强度 以及 微博间的联系强度，然后将其记录下来
        值得注意的是，不知道只统计了三个月的转发够不够，但再统计两年的转发就有点费时间了
        :return:
        """
        if type == 'user':
            # 首先生成以wblogId为key的转发边字典，{wblogId: [uid]}，方便后面计算
            retweet_edge = {}
            for uid in self.retweet_edge.keys():
                for wblogId in self.retweet_edge[uid]:
                    if wblogId not in retweet_edge:
                        retweet_edge[wblogId] = []
                    if uid not in retweet_edge[wblogId]:
                        retweet_edge[wblogId].append(uid)
            retweet_cnt = self.wblog_retweet_cnt  # 微博的retweet_cnt
            retweet_cnt2 = self.user_retweet_cnt  # 用户自己的retweet_cnt
        else:
            # 如果是计算微博的联系强度的话，就直接用原本的转发关系边就行
            retweet_edge = self.retweet_edge
            retweet_cnt = self.user_retweet_cnt
            retweet_cnt2 = self.wblog_retweet_cnt

        # 然后计算用户两两间的联系强度，并生成一个用户数*用户数的方阵S
        # 因为我后面会将这个方阵写入文件中，所以就不真正生成S了
        # 对于微博同理

        compute_relation_intensity(type, target, retweet_cnt, retweet_cnt2, retweet_edge, self.file_name_appendix,
                                   workers=workers)

        # nc = 0  # 记录方阵S中不为0的元素数
        #
        # with open('../main/relation_intensity/%s' % type + self.file_name_appendix + '.txt', 'w') as my_file:
        #     for i in range(len(target)):
        #         """
        #         这个地方太浪费时间了，想一想如何通过多进程实现
        #         """
        #         id1 = target[i]
        #         for j in range(i + 1, len(target)):
        #             id2 = target[j]
        #             # 计算id1和id2之间的联系强度
        #             s = 0.0
        #             if retweet_cnt2[id1] == 0 or retweet_cnt2[id2] == 0:
        #                 s = 0.0
        #             else:
        #                 for key in retweet_edge.keys():
        #                     if len(retweet_edge[key]) == 0:
        #                         continue
        #                     if id1 in retweet_edge[key] and id2 in retweet_edge[key]:
        #                         s += 1.0 / float(retweet_cnt[key])
        #             if s != 0.0:
        #                 nc += 1
        #                 my_file.write('%s %s %s\n' % (id1, id2, str(s)))
        # logging.info('%s, nc=%s' % (type, str(nc)))

    def setRelationIntensity_old(self, type, target):
        """
        读取记录下来的用户间的联系强度 以及 微博间的联系强度
        首先保存为稀疏矩阵A，然后计算A^T*A后保存为正常矩阵，再记录到文件中
        :return:
        """
        with open('../main/relation_intensity/%s' % type + self.file_name_appendix + '.txt', 'r') as my_file:
            row_and_column = len(my_file.readlines())
        A = sparse.lil_matrix((row_and_column, len(target)))
        with open('../main/relation_intensity/%s' % type + self.file_name_appendix + '.txt', 'r') as my_file:
            cnt = 0
            if type == 'user':
                retweet_cnt = self.user_retweet_cnt
            else:
                retweet_cnt = self.wblog_retweet_cnt
            for line in my_file:
                line = line.split('\n')[0]
                id1 = line.split(' ')[0]
                id2 = line.split(' ')[1]
                index1 = target.index(id1)
                index2 = target.index(id2)
                ri = line.split(' ')[2]

                A[cnt, index1] = pow(float(ri) / float(retweet_cnt[id1]), 0.5)
                A[cnt, index2] = 0.0 - pow(float(ri) / float(retweet_cnt[id2]), 0.5)
                cnt += 1

        logging.info('setRelationIntensity_old read file finished')
        if type == 'user':
            sparse.save_npz('../main/relation_intensity/A' + self.file_name_appendix, A.tocoo())
            logging.info('save A finished')
        else:
            sparse.save_npz('../main/relation_intensity/B' + self.file_name_appendix, A.tocoo())
            logging.info('save B finished')
        ATA = A.T.dot(A).tocoo()
        logging.info('setRelationIntensity_old ATA finished')
        if type == 'user':
            sparse.save_npz('../main/relation_intensity/ATA' + self.file_name_appendix, ATA)
            logging.info('save ATA finished')
        else:
            sparse.save_npz('../main/relation_intensity/BTB' + self.file_name_appendix, ATA)
            logging.info('save BTB finished')

    def setRelationIntensity(self, reset_dataset=False, workers=4):
        """
        reset_dataset为True的时候
        调用setRelationIntensity_new（user）和setRelationIntensity_new（wblog）

        reset_dataset为False的时候
        调用setRelationIntensity_old（user）和setRelationIntensity_old（wblog）
        :return:

        A
        B
        ATA
        BTB
        '../main/relation_intensity/user.txt'
        '../main/relation_intensity/wblog.txt'
        """
        # self.loadRetweetRelationship()
        """
        上面这个被我单独调用了， 见主程序
        """
        if reset_dataset:
            logging.info('setRelationIntensity_new------user')
            self.setRelationIntensity_new('user', self.all_user, workers=workers)
            logging.info('setRelationIntensity_new------wblog')
            self.setRelationIntensity_new('wblog', self.all_wblog, workers=workers)
            logging.info('setRelationIntensity_new------finished')

            logging.info('setRelationIntensity_old------user')
            self.setRelationIntensity_old('user', self.all_user)
            logging.info('setRelationIntensity_old------wblog')
            self.setRelationIntensity_old('wblog', self.all_wblog)
            logging.info('setRelationIntensity_old------finished')

    def setLaplacian(self):
        """
        计算拉普拉斯矩阵L，并保存进文件中
        :return: none
        """
        # 首先要计算用户的pagerank值
        # self.loadFollowRelationship()
        """
        上面这个被我单独调用了， 见主程序
        """
        logging.info('计算pagerank值')

        print("572注意啦啦啦啦！！！！！")
        import operator
        print('572', list(self.follow_edge.keys()))
        print('572', self.all_user)
        print('572', operator.eq(list(self.follow_edge.keys()), self.all_user))

        page_ranks = PRMapReduce(nodes=self.all_user, edge=self.follow_edge).page_rank()
        # 生成对角矩阵PI
        PI = sparse.lil_matrix((len(self.all_user), len(self.all_user)))
        for i in range(len(self.all_user)):
            PI[i, i] = float(page_ranks[self.all_user[i]][0])
        # 生成跳转概率矩阵P
        P = sparse.lil_matrix((len(self.all_user), len(self.all_user)))
        for uid in self.follow_edge.keys():
            for followeeUid in self.follow_edge[uid]:
                P[self.all_user.index(uid), self.all_user.index(followeeUid)] = 1.0 / float(self.follow_cnt[uid]) * 0.85
        for i in range(len(self.all_user)):
            for j in range(len(self.all_user)):
                P[i, j] += 0.15 * 1.0 / len(self.all_user)
        # 计算拉普拉斯矩阵L
        I = sparse.identity(len(self.all_user))
        # L = I - (PI.power(0.5) * P * PI.power(-0.5) + PI.power(-0.5) * P.T * PI.power(0.5)).dot(0.5)
        L = PI - (PI.dot(P) + P.T.dot(PI)).dot(0.5)
        L = L.tocoo()
        # 写入文件中
        sparse.save_npz('../main/relation_intensity/L' + self.file_name_appendix, L)
        logging.info('setLaplacian finished')

    def setReteetMatrix(self):
        """
        设置转发矩阵R，并保存进文件中
        :return: none
        """
        # self.loadRetweetRelationship()
        """
        上面这句话被我单独调用了，见主程序
        """
        # 生成转发矩阵R
        R = sparse.lil_matrix((len(self.all_user), len(self.all_wblog)))
        for uid in self.retweet_edge.keys():
            for wblogId in self.retweet_edge[uid]:
                R[self.all_user.index(uid), self.all_wblog.index(wblogId)] = 1.0
        R = R.tocoo()
        # 写入文件中
        sparse.save_npz('../main/relation_intensity/R' + self.file_name_appendix, R)
        logging.info('setReteetMatrix finished')

    def run(self, lenda1, lenda2, alpha, beta, gamma, theta, iteration_limit, change_limit):
        """
        跑MSCA算法
        :return:
        """
        # 首先确定x和y向量
        li = []
        for uid in self.user_train_list:
            li.append(float(self.user_train_dict[uid]['label']))
        for uid in self.user_prior_list:
            li.append(float(self.user_prior_dict[uid]['prior']))
            # li.append(-1)
        self.x_p = numpy.array(li)
        logging.info('user num: %s' % str(len(li)))
        li = []
        for wblogId in self.wblog_train_list:
            li.append(float(self.wblog_train_dict[wblogId]['label']))
        for wblogId in self.wblog_prior_list:
            li.append(float(self.wblog_prior_dict[wblogId]['prior']))
            # li.append(-1)
        self.y_p = numpy.array(li)
        logging.info('wblog num: %s' % str(len(li)))

        # 载入转发矩阵
        self.R = sparse.load_npz('../main/relation_intensity/R' + self.file_name_appendix + '.npz')
        # 然后需要分别计算x和y迭代时的逆矩阵
        logging.info('计算迭代x时的逆矩阵')
        self.I1 = sparse.identity(len(self.all_user))
        self.ATA = sparse.load_npz('../main/relation_intensity/ATA' + self.file_name_appendix + '.npz')
        self.L = sparse.load_npz('../main/relation_intensity/L' + self.file_name_appendix + '.npz')
        logging.info('计算迭代y时的逆矩阵')
        self.I2 = sparse.identity(len(self.all_wblog))
        self.BTB = sparse.load_npz('../main/relation_intensity/BTB' + self.file_name_appendix + '.npz')
        self.A = sparse.load_npz('../main/relation_intensity/A' + self.file_name_appendix + '.npz')
        self.B = sparse.load_npz('../main/relation_intensity/B' + self.file_name_appendix + '.npz')

        # # 首先确定x和y向量
        # li = []
        # for uid in self.user_train_list:
        #     li.append(float(self.user_train_dict[uid]['label']))
        # for uid in self.user_prior_list:
        #     li.append(float(self.user_prior_dict[uid]['prior_label']))
        # x_p = numpy.array(li)
        # logging.info('user num: %s' % str(len(li)))
        # li = []
        # for wblogId in self.wblog_train_list:
        #     li.append(float(self.wblog_train_dict[wblogId]['label']))
        # for wblogId in self.wblog_prior_list:
        #     li.append(float(self.wblog_prior_dict[wblogId]['prior_label']))
        # y_p = numpy.array(li)
        # logging.info('wblog num: %s' % str(len(li)))
        #
        # # 载入转发矩阵
        # R = sparse.load_npz('relation_intensity\\R.npz')
        #
        # # 然后需要分别计算x和y迭代时的逆矩阵
        # logging.info('计算迭代x时的逆矩阵')
        # I1 = sparse.identity(len(self.all_user))
        # ATA = sparse.load_npz('relation_intensity\\ATA.npz')
        # L = sparse.load_npz('relation_intensity\\L.npz')
        # xm = I1.dot(2.0 * lenda1) + ATA.dot(2.0 * alpha) + L.dot(2.0 * theta)
        # xm = linalg.inv(xm.toarray())
        # logging.info('计算迭代y时的逆矩阵')
        # I2 = sparse.identity(len(self.all_wblog))
        # BTB = sparse.load_npz('relation_intensity\\BTB.npz')
        # ym = I2.dot(2.0 * lenda2) + BTB.dot(2.0 * beta)
        # ym = linalg.inv(ym.toarray())
        #
        # A = sparse.load_npz('relation_intensity\\A.npz')
        # B = sparse.load_npz('relation_intensity\\B.npz')

        li = []
        for uid in self.all_user:
            li.append(0.0)
        w_o = numpy.array(li)
        C = sparse.lil_matrix((len(self.all_user), len(self.all_user)))
        for i in range(len(self.user_train_list)):
            C[i, i] = float(1.0)
        li = []
        for uid in self.user_train_list:
            li.append(float(self.user_train_dict[uid]['label']))
        for uid in self.user_prior_list:
            li.append(0.0)
        u = numpy.array(li)
        luo_x = 20.05
        xm = self.I1.dot(2.0 * lenda1) + self.ATA.dot(2.0 * alpha) + self.L.dot(2.0 * theta) + C.T.dot(C).dot(luo_x)

        # xm = self.I1.dot(2.0 * lenda1) + self.ATA.dot(2.0 * alpha) + self.L.dot(2.0 * theta)
        xm = linalg.inv(xm.toarray())

        li = []
        for wblogId in self.all_wblog:
            li.append(0.0)
        m_o = numpy.array(li)
        D = sparse.lil_matrix((len(self.all_wblog), len(self.all_wblog)))
        for i in range(len(self.wblog_train_list)):
            D[i, i] = float(1.0)
        li = []
        for wblogId in self.wblog_train_list:
            li.append(float(self.wblog_train_dict[wblogId]['label']))
        for wblogId in self.wblog_prior_list:
            li.append(0.0)
        m = numpy.array(li)
        luo_y = 5.05749
        ym = self.I2.dot(2.0 * lenda2) + self.BTB.dot(2.0 * beta) + D.T.dot(D).dot(luo_y)

        # ym = self.I2.dot(2.0 * lenda2) + self.BTB.dot(2.0 * beta)
        ym = linalg.inv(ym.toarray())

        # 开始迭代
        logging.info('开始迭代')
        iteration = 0
        x = self.x_p
        y = self.y_p
        cnt1 = 0
        cnt2 = 0
        while True:
            iteration += 1
            logging.info('iteration: %s' % str(iteration))
            if iteration > iteration_limit:
                break
            self.getFun(lenda1, lenda2, alpha, beta, gamma, theta, x, self.x_p, y, self.y_p, self.A, self.B, self.R,
                        self.L)

            iteration_x = 0
            w = w_o
            tmp = x
            while True:
                iteration_x += 1
                if iteration_x > 1000:
                    break
                x_next = xm.dot(
                    self.x_p.dot(2 * lenda1) + self.R.dot(gamma).dot(y) + C.T.dot(u).dot(luo_x) - C.T.dot(w))
                w_next = w + C.dot(x_next) - u
                change = self.getChange(tmp, x_next, w, w_next)
                tmp = x_next
                w = w_next
                # print(change)
                if change <= change_limit:
                    break
                cnt1 += 1
            # x_next = xm.dot(self.x_p.dot(2 * lenda1) + self.R.dot(gamma).dot(y))

            iteration_y = 0
            w = m_o
            tmp = y
            while True:
                iteration_y += 1
                if iteration_y > 100:
                    break
                y_next = ym.dot(
                    self.y_p.dot(2 * lenda2) + self.R.T.dot(gamma).dot(x_next) + D.T.dot(m).dot(luo_y) - D.T.dot(w))
                w_next = w + D.dot(y_next) - m
                change = self.getChange(tmp, y_next, w, w_next)
                tmp = y_next
                w = w_next
                if change <= change_limit:
                    break
                cnt2 += 1

            # y_next = ym.dot(self.y_p.dot(2 * lenda2) + self.R.T.dot(gamma).dot(x_next))

            change = self.getChange(x, x_next, y, y_next)
            logging.info('change: %s' % str(change))
            if change <= change_limit:
                break
            x = x_next
            y = y_next

            # for i in range(len(self.user_train_list)):
            #     x[i] = float(self.user_train_dict[self.user_train_list[i]]['label'])
            # for i in range(len(self.wblog_train_list)):
            #     y[i] = float(self.wblog_train_dict[self.wblog_train_list[i]]['label'])

        logging.info('迭代结束')
        print(cnt1)
        print(cnt2)
        # 将结果写入文件
        numpy.savetxt('res_user' + self.file_name_appendix + '.txt', x)
        numpy.savetxt('res_wblog' + self.file_name_appendix + '.txt', y)

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

    def getFun(self, lenda1, lenda2, alpha, beta, gamma, theta, x, x_p, y, y_p, A, B, R, L):
        """
        计算损失函数的值
        :return:
        """
        # print(pow(lenda1 * linalg.norm(x - x_p, 2), 2))
        # print(pow(lenda2 * linalg.norm(y - y_p, 2), 2))
        # print(pow(alpha * linalg.norm(A.dot(x), 2), 2))
        # print(pow(beta * linalg.norm(B.dot(y), 2), 2))
        # print(0.0 - gamma * R.T.dot(x).dot(y))
        # print(theta * L.T.dot(x).dot(x))

        res = pow(lenda1 * linalg.norm(x - x_p, 2), 2)
        res += pow(lenda2 * linalg.norm(y - y_p, 2), 2)
        res += pow(alpha * linalg.norm(A.dot(x), 2), 2)
        res += pow(beta * linalg.norm(B.dot(y), 2), 2)
        res -= gamma * R.T.dot(x).dot(y)
        res += theta * L.T.dot(x).dot(x)
        logging.info('Function loss: %s' % str(res))

    def evaluation_bak(self):
        """
        评价MSCA算法的结果
        :return:
        """
        logging.info('用户结果')
        scores = []
        cnt = 0
        with open('../main/res_user' + self.file_name_appendix + '.txt', 'r') as my_file:
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

        # roc
        fpr, tpr, thresholds = metrics.roc_curve(test_result, scores)
        logging.info('user AUC:%s' % str(metrics.auc(fpr, tpr)))
        with open('../main/user_roc' + self.file_name_appendix + '.txt', 'w') as my_file:
            my_file.write('fpr tpr\n')
            for i in range(len(fpr)):
                my_file.write('%s %s\n' % (str(fpr[i]), str(tpr[i])))

    def evaluation(self):
        """
        评价MSCA算法的结果
        :return:
        """
        logging.info('用户结果')
        scores = []
        cnt = 0
        with open('../main/res_user' + self.file_name_appendix + '.txt', 'r') as my_file:
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
        with open('../main/user_ap' + self.file_name_appendix + '.txt', 'w') as my_file:
            my_file.write('p r\n')
            for i in range(len(p)):
                my_file.write('%s %s\n' % (str(p[i]), str(r[i])))

        # roc
        fpr, tpr, thresholds = metrics.roc_curve(test_result, scores)
        logging.info('user AUC:%s' % str(metrics.auc(fpr, tpr)))
        with open('../main/user_roc' + self.file_name_appendix + '.txt', 'w') as my_file:
            my_file.write('fpr tpr\n')
            for i in range(len(fpr)):
                my_file.write('%s %s\n' % (str(fpr[i]), str(tpr[i])))

        # top k precision
        worker_score = {}
        for i in range(len(scores)):
            worker_score[self.user_prior_list[i]] = scores[i]
        worker_score = sorted(worker_score.items(), key=lambda im: float(im[1]), reverse=True)
        with open('../main/res_user_top' + self.file_name_appendix + '.txt', 'w') as my_file:
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
        with open('../main/res_wblog' + self.file_name_appendix + '.txt', 'r') as my_file:
            for line in my_file:
                score = float(line.split('\n')[0])
                if self.all_wblog[cnt] in self.wblog_prior_list:
                    scores.append(score)
                cnt += 1
        logging.info(
            'min_score: %s, max_score: %s, len(wblog):%s' % (str(min(scores)), str(max(scores)), str(len(scores))))
        test_result = []
        for wblogId in self.wblog_prior_list:
            test_result.append(int(self.wblog_prior_dict[wblogId]['label']))
        wblog_res = Evaluation.evaluation_self(scores, test_result)

        # top k precision
        wblog_score = {}
        for i in range(len(scores)):
            wblog_score[self.wblog_prior_list[i]] = scores[i]
        wblog_score = sorted(wblog_score.items(), key=lambda im: float(im[1]), reverse=True)
        with open('../main/res_wblog_top' + self.file_name_appendix + '.txt', 'w') as my_file:
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

        # ap
        p, r, thresholds = metrics.precision_recall_curve(test_result, scores)
        ap = metrics.average_precision_score(test_result, scores)
        logging.info('wblog AP:%s' % str(ap))
        with open('../main/wblog_ap' + self.file_name_appendix + '.txt', 'w') as my_file:
            my_file.write('p r\n')
            for i in range(len(p)):
                my_file.write('%s %s\n' % (str(p[i]), str(r[i])))

        # roc
        fpr, tpr, thresholds = metrics.roc_curve(test_result, scores)
        logging.info('wblog AUC:%s' % str(metrics.auc(fpr, tpr)))
        with open('../main/wblog_roc' + self.file_name_appendix + '.txt', 'w') as my_file:
            my_file.write('fpr tpr\n')
            for i in range(len(fpr)):
                my_file.write('%s %s\n' % (str(fpr[i]), str(tpr[i])))

        return user_res, wblog_res

    def show(self):
        """
        为了界面展示
        :return:
        """
        self.all_user = random.sample(self.all_user, 500)
        self.all_wblog = random.sample(self.all_wblog, 500)
        for uid in self.all_user:
            self.retweet_edge[uid] = []
            for res in self.sqlhelper.select_sql('SELECT paMid, orMid FROM wblog WHERE uid=%s' % uid):
                paMid = str(res[0])
                orMid = str(res[1])
                if paMid in self.all_wblog:
                    self.retweet_edge[uid].append(paMid)
                if orMid in self.all_wblog:
                    self.retweet_edge[uid].append(orMid)
        mdb = MongoClient().comment.comment
        for wblogId in self.swblog:
            for res in mdb.find({'wblogId': wblogId}):
                try:
                    uid = res['json_text']['user']['id']
                    if uid in self.retweet_edge.keys():
                        if wblogId not in self.retweet_edge[uid]:
                            self.retweet_edge[uid].append(wblogId)
                except Exception as e:
                    logging.error('%s. The wblogId is %s' % (e, str(wblogId)))


if __name__ == '__main__':
    # # 为了测试numpy
    # li = [1,2,3,4,5]
    # a = numpy.array(li)
    # b = a
    # print(a / 2)
    # import PyQt5
    # from PyQt5 import QtCore
    # from PyQt5 import QtGui
    #
    # logging.basicConfig(level=logging.INFO,
    #                     format='%(asctime)s : %(levelname)s  %(message)s',
    #                     datefmt='%Y-%m-%d %A %H:%M:%S')

    logging.info('开始跑CMSCA算法')
    msca = MSCA('localhost', 'sdh', 'root', 'root', 'utf8')

    # logging.info('计算拉普拉斯矩阵L')
    # msca.setLaplacian()
    #
    # logging.info('计算转发矩阵R')
    # msca.setReteetMatrix()
    #
    # logging.info('计算 用户间的 && 微博间的 联系强度')
    # msca.setRelationIntensity(reset_dataset=True)
    # msca.setRelationIntensity(reset_dataset=False)

    msca.run(lenda1=1.0, lenda2=1.0, alpha=0.15, beta=0.75, gamma=0.04, theta=1650, iteration_limit=100,
             change_limit=0.01)

    logging.info('MSCA算法结果')
    msca.evaluation()
