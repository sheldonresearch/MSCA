#!usr/bin/env python  
# -*- coding:utf-8 _*-  
""" 
@project:MSCA
@author:xiangguosun 
@contact:sunxiangguodut@qq.com
@website:http://blog.csdn.net/github_36326955
@file: test.py 
@platform: macOS High Sierra 10.13.1 Pycharm pro 2017.1 
@time: 2019/05/13 
"""

# 跑MSCA算法
import logging
from pymongo import MongoClient
from src.algorithm.alkit import Alkit
from src.util.SqlHelper import SqlHelper


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

    def loadRetweetRelationship(self):
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
        # logging.info('读取转发关系')  # 3884个用户全部处理完大概需要30min
        # # """
        # #       采样了
        # #
        # #       """
        # uid_count = 0
        # # all_user_sample=self.all_user[0:10]
        # # print("len(all_user_sample)",len(all_user_sample))
        # for uid in self.all_user:
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
        #
        # """
        # save self.retweet_edge
        # """
        #
        # mdb = MongoClient().comment.comment
        # # print('len self.swblog: ',len(self.swblog))
        # # """
        # # 采样了
        # #
        # # """
        # # sw_sample=self.swblog[0:10]
        # for wblogId in self.swblog:
        #     # for wblogId in sw_sample:
        #     for res in mdb.find({'wblogId': wblogId}):
        #         try:
        #             uid = res['json_text']['user']['id']
        #             if uid in self.retweet_edge.keys():
        #                 if wblogId not in self.retweet_edge[uid]:
        #                     self.retweet_edge[uid].append(wblogId)
        #         except Exception as e:
        #             logging.error('%s. The wblogId is %s' % (e, str(wblogId)))
        #
        # # print("length retweet_edge: ",len(self.retweet_edge))
        #
        # logging.info('读取微博的转发数')
        # # 读取每条微博的转发数，方便后面计算用户间的联系强度
        # # print(len(self.all_wblog))
        # for wblogId in self.all_wblog:
        #     self.wblog_retweet_cnt[wblogId] = 0
        # for uid in self.retweet_edge.keys():
        #     for wblogId in self.retweet_edge[uid]:
        #         self.wblog_retweet_cnt[wblogId] += 1

        # 下面是统计一条微博总的转发数，也即转发数会很大

        mdb1 = MongoClient().wblog.wblog
        mdb2 = MongoClient().wblog.swblog
        wblogId_list = ['4041807245070296',
                        '3840315800968256',
                        '3991386728698754',
                        '4002250903441477']
        for wblogId in wblogId_list:
            wblog1=mdb1.find_one({'wblogId': wblogId})
            print(wblog1)
            wblog2=mdb2.find_one({'wblogId': wblogId})
            print(wblog2)


        # for wblogId in self.all_wblog:
        #     self.wblog_retweet_cnt[wblogId] = 0
        #
        #
        #
        # suc = 0
        # fail = 0
        # for wblogId in self.all_wblog:
        #     try:
        #         wblog = mdb1.find_one({'wblogId': wblogId})
        #         self.wblog_retweet_cnt[wblogId] = int(wblog['json_text']['reposts_count'])
        #         wblog = mdb2.find_one({'wblogId': wblogId})
        #         self.wblog_retweet_cnt[wblogId] = int(wblog['json_text']['reposts_count'])
        #         suc = suc + 1
        #         print("LINE:172 | suc: ", suc, "fail: ", fail)
        #     except Exception as e:
        #         fail = fail + 1
        #         logging.error('%s. The wblogId is %s' % (e, str(wblogId)))
        # logging.error('success %s, fail %s' % (str(suc), str(fail)))

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
        #         # logging.error('%s. The wblogId is %s' % (e, str(wblogId)))
        # logging.error('for wblogId in self.nwblog... success %s, fail %s' % (str(suc), str(fail)))
        #
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
        #
        # logging.info('读取用户的转发数')
        # # 同样的，读取每个用户的转发数，方便后面计算微博间的联系强度
        # # 由于用户的转发数和微博的转发数的获取难度不同，后者在json里就有，前者没有，所以我就只统计这三个月的了
        # for uid in self.all_user:
        #     self.user_retweet_cnt[uid] = len(self.retweet_edge[uid])
        # logging.info('loadRetweetRelationship finished')


if __name__ == '__main__':
    logger = logging.getLogger()
    logger.setLevel('INFO')
    BASIC_FORMAT = '%(asctime)s : %(levelname)s  %(message)s'
    DATE_FORMAT = '%Y-%m-%d %A %H:%M:%S'
    formatter = logging.Formatter(BASIC_FORMAT, DATE_FORMAT)
    chlr = logging.StreamHandler()  # 输出到控制台的handler
    chlr.setFormatter(formatter)
    chlr.setLevel('INFO')  # 也可以不设置，不设置就默认用logger的level
    fhlr = logging.FileHandler('results.log')  # 输出到文件的handler
    fhlr.setFormatter(formatter)
    logger.addHandler(chlr)
    logger.addHandler(fhlr)

    train_per = 0.8
    spammer_per = 0.9
    spam_per = 0.9
    reset_dataset = True
    dump = True
    add_unknown_into_model = False

    file_name_appendix = '_tp_' + str(train_per) + \
                         '_spammer_' + str(spammer_per) + \
                         '_spam_' + str(spam_per) + \
                         '_addunknown_' + str(add_unknown_into_model)

    msca = MSCA(h='localhost', d='sdh', u='root', p='root', c='utf8',
                file_name_appendix=file_name_appendix)

    msca.loadRetweetRelationship()
