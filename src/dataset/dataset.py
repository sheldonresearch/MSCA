# 从mysql表中抽取数据集（从一万多用户中抽取一部分作为数据集，还有抽取微博）
import logging
import random
from src.util.SqlHelper import SqlHelper
from pymongo import MongoClient

class Dataset(object):

    def __init__(self, h, d, u, p, c):
        self.host = h
        self.db = d
        self.user = u
        self.passwd = p
        self.charset = c
        self.sqlhelper = SqlHelper(host=self.host, db=self.db, user=self.user, passwd=self.passwd, charset=self.charset)

    def userset(self):
        """
        从一万多用户中抽取一部分作为数据集
        量级为3000，尽量抽取那些在wblog表中发布过微博的用户
        发布过微博：没有发布过微博 = 9：1
        :return:
        """
        logging.info('开始抽取用户数据集')
        total_user = self.sqlhelper.select_sql_one('SELECT uid FROM normal')
        user_with_blog = []
        for user in total_user:
            if self.sqlhelper.select_sql_exist('SELECT uid FROM wblog WHERE uid=%s' % user):
                if int(self.sqlhelper.select_sql_first('SELECT nowPage FROM normal WHERE uid=%s' % user)) < 1000:
                    user_with_blog.append(user)
        logging.info('len(user_with_blog): ' + str(len(user_with_blog)))
        # user_set = []
        # for user in MongoClient().profile.user.find():
        #     try:
        #         user['json_text']['followers_count']
        #     except Exception as e:
        #         logging.error('%s. The user is %s' % (e, str(uid)))
        user_set = random.sample(user_with_blog, 2700)
        for user in total_user:
            if user not in user_set:
                user_set.append(user)
                if len(user_set) == 3000:
                    break
        for user in user_set:
            self.sqlhelper.insert_or_update_sql('UPDATE normal SET choose="yes" WHERE uid=%s' % user)

    def wblogset(self):
        """
        从150万条微博中抽取一部分作为数据集
        量级为4000，抽取规则为排除法，即
        1.首先选取userset中的用户转发过的微博（通过wblog表中的orMid来确定）
        2.非原创微博不选(通过1可以保证)
        3.评论要大于一页的
        :return:
        """
        logging.info('开始抽取微博数据集')
        user_set = self.sqlhelper.select_sql_one('SELECT uid FROM spammer') + \
                   self.sqlhelper.select_sql_one('SELECT uid FROM normal WHERE choose="yes"')
        wblog_set = []
        for uid in user_set:
            for orMid in self.sqlhelper.select_sql_one('SELECT orMid FROM wblog WHERE uid=%s' % uid):
                if orMid != '0':
                    wblog_set.append(orMid)
        logging.info('len(wblog_set): ' + str(len(wblog_set)))

        tmp_wblog_set = []
        for itm in wblog_set:
            tmp_wblog_set.append(itm)
        wblog_set = []

        for wblogId in tmp_wblog_set:
            maxPage = int(self.sqlhelper.select_sql_first('SELECT maxPage FROM wblog WHERE wblogId=%s' % wblogId))
            if maxPage >= 1:
                wblog_set.append(wblogId)
        logging.info('len(wblog_set): ' + str(len(wblog_set)))

        wblog_set = random.sample(wblog_set, 4000)

        for wblogId in wblog_set:
            for res in self.sqlhelper.select_sql('SELECT uid, wblogId FROM wblog WHERE wblogId=%s' % wblogId):
                uid = str(res[0])
                wblogId = str(res[1])
            self.sqlhelper.insert_or_update_sql('INSERT INTO wblog_choose SET uid=%s, wblogId=%s' % (uid, wblogId))

    def commentset(self):
        """
        发现以前从comment.json_text中抽取评论再写入comment.comment有错误，表现为有一些评论莫名奇妙没加进来
        再加上重新选择了微博，所以准备重新生成一张comment.comment
        :return: none
        """
        my_collection = MongoClient().comment.json_text
        new_collection = MongoClient().comment.comment
        for res in self.sqlhelper.select_sql('SELECT wblogId FROM swblog'):
            wblogId = res[0]
            for comment in my_collection.find({'wblogId': str(wblogId)}):
                try:
                    for each_comment in comment['json_text']:
                        try:
                            if each_comment['user']['id']:
                                uid = each_comment['user']['id']
                                new_collection.insert_one(
                                    {'uid': str(uid), 'wblogId': str(wblogId), 'json_text': each_comment})
                        except Exception as e:
                            print('no user %s, the wblogId is %s' % (e, str(wblogId)))
                except Exception as e:
                    print('unknown error %s, the wblogId is %s' % (e, str(wblogId)))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s : %(levelname)s  %(message)s',
                        datefmt='%Y-%m-%d %A %H:%M:%S')

    ds = Dataset('localhost', 'sdh', 'root', 'root', 'utf8')
    # ds.userset()
    # ds.wblogset()
    ds.commentset()