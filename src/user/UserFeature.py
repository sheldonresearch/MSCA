# 实现用户的特征提取功能
import pymongo
from pymongo import MongoClient
import logging
from src.util.SqlHelper import SqlHelper
import math
import datetime


class UserFeature:
    def __init__(self, h, d, u, p, c):  # 'localhost', 'sdh', 'root', 'root', 'utf8'
        self.host = h
        self.db = d
        self.user = u
        self.passwd = p
        self.charset = c

    def __enter__(self):
        self.sqlhelper = SqlHelper(host=self.host, db=self.db, user=self.user, passwd=self.passwd, charset=self.charset)
        self.mdb = MongoClient().userFeature
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.sqlhelper.close()

    def arrangeFeatures(self):
        """
        将多张特征表整合为一个表，方便后面使用pandas操作
        :return:
        """
        col = self.mdb.features
        if not col.find_one():
            logging.info('features为空')
            col.create_index([('uid', pymongo.DESCENDING)], unique=True)

    def setRegisterDay(self):
        """
        设置用户的注册天数 和 log后的结果
        :return: none
        """
        col = self.mdb.registerDay
        if not col.find_one():
            logging.info('registerDay为空，设置主键为uid')
            col.create_index([('uid', pymongo.DESCENDING)], unique=True)

        # spammers = self.sqlhelper.select_sql_one('SELECT uid FROM spammer')
        # normal = self.sqlhelper.select_sql_one('SELECT uid FROM normal WHERE choose="yes"')

        """我的修改：
        事实上，如果把choose='yes'去掉， 那么mongodb里存储的就是所有的14774个账号的了。
        """
        spammers = self.sqlhelper.select_sql_one('SELECT uid FROM spammer')
        normal = self.sqlhelper.select_sql_one('SELECT uid FROM normal WHERE choose="yes"')
        unknown = self.sqlhelper.select_sql_one('SELECT uid FROM normal WHERE choose="not"')
        final_user = self.sqlhelper.select_sql_one('SELECT uid FROM final_user WHERE spammer="yes"')

        for uid in final_user:
            if uid not in spammers:
                spammers.append(uid)

        """
        到这为止, 代码中spammer相当于数据表里spammer U final_user.spammer一共有903
        """
        # 不知道为什么spammer和normal两个集合有重合的用户
        # 所以这里简单地将这些重合的用户都认为是spammer
        for uid in spammers:
            if uid in normal:
                normal.remove(uid)
            if uid in unknown:
                unknown.remove(uid)
        """
        到目前为止，我们得到了下面几个有用的东西
        spammer： 水军  
        normal： 正常用户
        unkonwn：还没来得及标注的未知类型用户
        """
        all_user = spammers + normal + unknown


        for uid in all_user:
            try:
                for card in MongoClient().profile.json_text.find_one({'uid': str(uid)})['json_text']['cards']:
                    if 'card_group' not in card:
                        continue
                    for elem in card['card_group']:
                        if 'item_name' in elem and elem['item_name'] == u'注册时间':
                            t = float((datetime.datetime(2017, 11, 25) - datetime.datetime.strptime(elem['item_content'], '%Y-%m-%d')).days)
                            if uid in spammers:
                                col.insert_one({'uid': uid, 'spammer': 'true', 'register_day': t, 'log_time': math.log10(t)})
                            elif uid in normal:
                                col.insert_one({'uid': uid, 'spammer': 'false', 'register_day': t, 'log_time': math.log10(t)})
                            elif uid in unknown:
                                col.insert_one({'uid': uid, 'spammer': 'unknown', 'register_day': t, 'log_time': math.log10(t)})
                            break
            except Exception as e:
                logging.error('%s. The user is %s' % (e, str(uid)))
        logging.info('setRegisterDay finished')

    def setFollowCnt(self):
        """
        设置用户的关注数,粉丝数和 log 后的结果
        :return: none
        """
        col = self.mdb.followCnt
        if not col.find_one():
            logging.info('followCnt为空，设置主键为uid')
            col.create_index([('uid', pymongo.DESCENDING)], unique=True)

        # spammers = self.sqlhelper.select_sql_one('SELECT uid FROM spammer')
        # normal = self.sqlhelper.select_sql_one('SELECT uid FROM normal WHERE choose="yes"')

        spammers = self.sqlhelper.select_sql_one('SELECT uid FROM spammer')
        normal = self.sqlhelper.select_sql_one('SELECT uid FROM normal WHERE choose="yes"')
        unknown = self.sqlhelper.select_sql_one('SELECT uid FROM normal WHERE choose="not"')
        final_user = self.sqlhelper.select_sql_one('SELECT uid FROM final_user WHERE spammer="yes"')

        for uid in final_user:
            if uid not in spammers:
                spammers.append(uid)

        """
        到这为止, 代码中spammer相当于数据表里spammer U final_user.spammer一共有903
        """
        # 不知道为什么spammer和normal两个集合有重合的用户
        # 所以这里简单地将这些重合的用户都认为是spammer
        for uid in spammers:
            if uid in normal:
                normal.remove(uid)
            if uid in unknown:
                unknown.remove(uid)
        """
        到目前为止，我们得到了下面几个有用的东西
        spammer： 水军  
        normal： 正常用户
        unkonwn：还没来得及标注的未知类型用户
        """

        for user in MongoClient().profile.user.find():
            uid = user['uid']
            try:
                if uid in spammers:
                    col.insert_one({'uid': uid, 'spammer': 'true',
                                    'followee_cnt': user['json_text']['follow_count'],
                                    'log_followee': math.log10(int(user['json_text']['follow_count'] + 1.0)),
                                    'follower_cnt': user['json_text']['followers_count'],
                                    'log_follower': math.log10(int(user['json_text']['followers_count'] + 1.0))})
                elif uid in normal:
                    col.insert_one({'uid': uid, 'spammer': 'false',
                                    'followee_cnt': user['json_text']['follow_count'],
                                    'log_followee': math.log10(int(user['json_text']['follow_count'] + 1.0)),
                                    'follower_cnt': user['json_text']['followers_count'],
                                    'log_follower': math.log10(int(user['json_text']['followers_count'] + 1.0))})
                elif uid in unknown:
                    col.insert_one({'uid': uid, 'spammer': 'unknown',
                                    'followee_cnt': user['json_text']['follow_count'],
                                    'log_followee': math.log10(int(user['json_text']['follow_count'] + 1.0)),
                                    'follower_cnt': user['json_text']['followers_count'],
                                    'log_follower': math.log10(int(user['json_text']['followers_count'] + 1.0))})
            except Exception as e:
                logging.error('%s. The user is %s' % (e, str(uid)))
        logging.info('setFollowCnt finished')

    def setRVP(self):
        """
        设置用户的双向关注率
        :return: none
        """
        col = self.mdb.rvp
        if not col.find_one():
            logging.info('rvp为空，设置主键为uid')
            col.create_index([('uid', pymongo.DESCENDING)], unique=True)

        # spammers = self.sqlhelper.select_sql_one('SELECT uid FROM spammer')
        # normal = self.sqlhelper.select_sql_one('SELECT uid FROM normal WHERE choose="yes"')
        # all_user = spammers + normal

        """我的修改：
                事实上，如果把choose='yes'去掉， 那么mongodb里存储的就是所有的14774个账号的了。
                """
        spammers = self.sqlhelper.select_sql_one('SELECT uid FROM spammer')
        normal = self.sqlhelper.select_sql_one('SELECT uid FROM normal WHERE choose="yes"')
        unknown = self.sqlhelper.select_sql_one('SELECT uid FROM normal WHERE choose="not"')
        final_user = self.sqlhelper.select_sql_one('SELECT uid FROM final_user WHERE spammer="yes"')

        for uid in final_user:
            if uid not in spammers:
                spammers.append(uid)

        """
        到这为止, 代码中spammer相当于数据表里spammer U final_user.spammer一共有903
        """
        # 不知道为什么spammer和normal两个集合有重合的用户
        # 所以这里简单地将这些重合的用户都认为是spammer
        for uid in spammers:
            if uid in normal:
                normal.remove(uid)
            if uid in unknown:
                unknown.remove(uid)
        """
        到目前为止，我们得到了下面几个有用的东西
        spammer： 水军  
        normal： 正常用户
        unkonwn：还没来得及标注的未知类型用户
        """
        all_user = spammers + normal + unknown

        edge = {}
        for uid in all_user:
            for result in self.sqlhelper.select_sql('SELECT uid, followeeUid FROM edge WHERE uid=%s' % uid):
                if result[0] in edge.keys():
                    edge[result[0]].append(result[1])
                else:
                    edge[result[0]] = [result[1]]
        edge_reverse = {}
        for uid in all_user:
            for result in self.sqlhelper.select_sql('SELECT uid, followeeUid FROM edge WHERE followeeUid=%s' % uid):
                if result[1] in edge_reverse.keys():
                    edge_reverse[result[1]].append(result[0])
                else:
                    edge_reverse[result[1]] = [result[0]]

        for uid in all_user:
            res = UserFeature.caculate_rvp_ratio(int(uid), edge, edge_reverse)
            try:
                if uid in spammers:
                    col.insert_one({'uid': uid, 'spammer': 'true', 'rvp_ratio': str(res)})
                elif uid in normal:
                    col.insert_one({'uid': uid, 'spammer': 'false', 'rvp_ratio': str(res)})
                elif uid in unknown:
                    col.insert_one({'uid': uid, 'spammer': 'unknown', 'rvp_ratio': str(res)})
            except Exception as e:
                logging.error('%s. The user is %s' % (e, str(uid)))
        logging.info('setRVP finished')

    def setOriThirdFre(self):
        """
        设置用户发布微博时使用第三方软件的频率
        :return: none
        """
        third_party = ('推兔',
                       '好保姆',
                       '互粉派对 ',
                       '优推推互粉',
                       '未通过审核应用',
                       '互粉加加',
                       '互粉小助手',
                       '孔明社交管理',
                       '互粉赏金榜',
                       '推米互粉',
                       '多推',
                       '互粉一族',
                       '推兔手机版',
                       '推啊')

        col = self.mdb.oriThirdFre
        if not col.find_one():
            logging.info('oriThirdFre为空，设置主键为uid')
            col.create_index([('uid', pymongo.DESCENDING)], unique=True)

        ori_cnt = 0
        thi_cnt = 0
        ori_cnt_re = 0
        thi_cnt_re = 0
        # spammers = self.sqlhelper.select_sql_one('SELECT uid FROM spammer')
        # normal = self.sqlhelper.select_sql_one('SELECT uid FROM normal WHERE choose="yes"')
        spammers = self.sqlhelper.select_sql_one('SELECT uid FROM spammer')
        normal = self.sqlhelper.select_sql_one('SELECT uid FROM normal WHERE choose="yes"')
        unknown = self.sqlhelper.select_sql_one('SELECT uid FROM normal WHERE choose="not"')
        final_user = self.sqlhelper.select_sql_one('SELECT uid FROM final_user WHERE spammer="yes"')

        for uid in final_user:
            if uid not in spammers:
                spammers.append(uid)

        """
        到这为止, 代码中spammer相当于数据表里spammer U final_user.spammer一共有903
        """
        # 不知道为什么spammer和normal两个集合有重合的用户
        # 所以这里简单地将这些重合的用户都认为是spammer
        for uid in spammers:
            if uid in normal:
                normal.remove(uid)
            if uid in unknown:
                unknown.remove(uid)
        """
        到目前为止，我们得到了下面几个有用的东西
        spammer： 水军  
        normal： 正常用户
        unkonwn：还没来得及标注的未知类型用户
        """





        for user in MongoClient().profile.user.find():
            uid = user['uid']
            tmp_ori_cnt = 0  # 微博数量
            tmp_thi_cnt = 0  # 第三方微博数量
            tmp_ori_cnt_re = 0  # 微博数量（去除转发微博）
            tmp_thi_cnt_re = 0  # 第三方微博数量（去除转发微博）
            for res in self.sqlhelper.select_sql('SELECT source, retweet_flag FROM wblog WHERE uid=%s' % uid):
                source = res[0]
                retweet_flag = res[1]
                # 下面这个判断是为了筛选出原创微博
                if str(retweet_flag) == '0':
                    tmp_ori_cnt_re += 1
                    ori_cnt_re += 1
                    if source in third_party:
                        tmp_thi_cnt_re += 1
                        thi_cnt_re += 1
                tmp_ori_cnt += 1
                ori_cnt += 1
                if source in third_party:
                    tmp_thi_cnt += 1
                    thi_cnt += 1
            try:
                if uid in spammers:
                    col.insert_one({'uid': uid, 'spammer': 'true', 'ori_cnt-re': tmp_ori_cnt_re, 'thi_cnt-re': tmp_thi_cnt_re,
                                    'ori_cnt': tmp_ori_cnt, 'thi_cnt': tmp_thi_cnt})
                elif uid in normal:
                    col.insert_one({'uid': uid, 'spammer': 'false', 'ori_cnt-re': tmp_ori_cnt_re, 'thi_cnt-re': tmp_thi_cnt_re,
                                    'ori_cnt': tmp_ori_cnt, 'thi_cnt': tmp_thi_cnt})
                elif uid in unknown:
                    col.insert_one(
                        {'uid': uid, 'spammer': 'unknown', 'ori_cnt-re': tmp_ori_cnt_re, 'thi_cnt-re': tmp_thi_cnt_re,
                         'ori_cnt': tmp_ori_cnt, 'thi_cnt': tmp_thi_cnt})
            except Exception as e:
                print('%s. The user is %s' % (e, str(uid)))

        self.updateOriThirdFre(ori_cnt, thi_cnt, ori_cnt_re, thi_cnt_re)

    def updateOriThirdFre(self, ori_cnt, thi_cnt, ori_cnt_re, thi_cnt_re):
        """
        在setOriThirdFre中只是做了初步的统计
        所以这里需要计算出特征具体的值，并更新到mongodb中
        :return: none
        """
        col = self.mdb.oriThirdFre
        # ori_cnt = 1525387
        # thi_cnt = 47284
        # ori_cnt_re = 971792
        # thi_cnt_re = 10407

        max_ori = 0
        max_ori_re = 0
        for user in col.find():
            if user['ori_cnt'] > max_ori:
                max_ori = user['ori_cnt']
            if int(user['ori_cnt-re']) > max_ori_re:
                max_ori_re = user['ori_cnt-re']

        for user in col.find():
            if user['ori_cnt'] == 0:
                fre = float(thi_cnt) / ori_cnt
            else:
                coefficient = math.log10(user['ori_cnt'] + 1.0) / math.log10(max_ori)
                fre = coefficient * (float(user['thi_cnt']) / user['ori_cnt']) + (1 - coefficient) * (float(thi_cnt) / ori_cnt)
            col.update({'uid': user['uid']}, {'$set': {'fre': fre}})

            if user['ori_cnt'] == 0:
                fre = 0
            else:
                fre = float(user['thi_cnt']) / user['ori_cnt']
            col.update({'uid': user['uid']}, {'$set': {'fre_new': fre}})

        for user in col.find():
            if user['ori_cnt-re'] == 0:
                fre_re = float(thi_cnt_re) / ori_cnt_re
            else:
                coefficient = math.log10(user['ori_cnt-re'] + 1.0) / math.log10(max_ori_re)
                fre_re = coefficient * (float(user['thi_cnt-re']) / user['ori_cnt-re']) + (1 - coefficient) * (
                float(thi_cnt_re) / ori_cnt_re)
            col.update({'uid': user['uid']}, {'$set': {'fre-re': fre_re}})

    def setRetweetFre(self):
        """
        设置用户转发微博的关注比例
        :return: none
        """
        col = self.mdb.retweetFre
        if not col.find_one():
            logging.info('retweetFre为空，设置主键为uid')
            col.create_index([('uid', pymongo.DESCENDING)], unique=True)

        # spammers = self.sqlhelper.select_sql_one('SELECT uid FROM spammer')
        # normal = self.sqlhelper.select_sql_one('SELECT uid FROM normal WHERE choose="yes"')

        """我的修改：
                      事实上，如果把choose='yes'去掉， 那么mongodb里存储的就是所有的14774个账号的了。
                      """
        spammers = self.sqlhelper.select_sql_one('SELECT uid FROM spammer')
        normal = self.sqlhelper.select_sql_one('SELECT uid FROM normal WHERE choose="yes"')
        unknown = self.sqlhelper.select_sql_one('SELECT uid FROM normal WHERE choose="not"')
        final_user = self.sqlhelper.select_sql_one('SELECT uid FROM final_user WHERE spammer="yes"')

        for uid in final_user:
            if uid not in spammers:
                spammers.append(uid)

        """
        到这为止, 代码中spammer相当于数据表里spammer U final_user.spammer一共有903
        """
        # 不知道为什么spammer和normal两个集合有重合的用户
        # 所以这里简单地将这些重合的用户都认为是spammer
        for uid in spammers:
            if uid in normal:
                normal.remove(uid)
            if uid in unknown:
                unknown.remove(uid)
        """
        到目前为止，我们得到了下面几个有用的东西
        spammer： 水军  
        normal： 正常用户
        unkonwn：还没来得及标注的未知类型用户
        """


        retweet_cnt = 0
        follow_cnt = 0
        onehop_cnt = 0
        for user in MongoClient().profile.user.find():
            uid = user['uid']
            tmp_retweet_cnt = 0
            tmp_follow_cnt = 0
            tmp_onehop_cnt = 0
            for res in self.sqlhelper.select_sql('SELECT retweet_flag, follow_flag, paMid FROM wblog WHERE uid=%s' % uid):
                retweet_flag = res[0]
                follow_flag = res[1]
                paMid = res[2]
                # 下面这个判断是为了筛选出转发微博
                if str(retweet_flag) == '0':
                    continue

                tmp_retweet_cnt += 1
                retweet_cnt += 1
                if str(follow_flag) == '1':
                    tmp_follow_cnt += 1
                    follow_cnt += 1
                if str(paMid) == '0':
                    tmp_onehop_cnt += 1
                    onehop_cnt += 1
            try:
                if uid in spammers:
                    col.insert_one(
                        {'uid': uid, 'spammer': 'true', 'retweet_cnt': tmp_retweet_cnt, 'follow_cnt': tmp_follow_cnt,
                         'onehop_cnt': tmp_onehop_cnt})
                elif uid in normal:
                    col.insert_one(
                        {'uid': uid, 'spammer': 'false', 'retweet_cnt': tmp_retweet_cnt, 'follow_cnt': tmp_follow_cnt,
                         'onehop_cnt': tmp_onehop_cnt})
                elif uid in unknown:
                    col.insert_one(
                        {'uid': uid, 'spammer': 'unknown', 'retweet_cnt': tmp_retweet_cnt, 'follow_cnt': tmp_follow_cnt,
                         'onehop_cnt': tmp_onehop_cnt})

            except Exception as e:
                print('%s. The user is %s' % (e, str(uid)))
        self.updateRetweetFre(retweet_cnt, follow_cnt, onehop_cnt)

    def updateRetweetFre(self, retweet_cnt, follow_cnt, onehop_cnt):
        """
        在setRetweetFre中只是做了初步的统计
        所以这里需要计算出特征具体的值，并更新到mongodb中
        :return: none
        """
        col = self.mdb.retweetFre

        # max_retweet_cnt = 0
        # max_follow_cnt = 0
        # max_onehop_cnt = 0
        # for user in col.find():
        #     if int(user['retweet_cnt']) > max_retweet_cnt:
        #         max_retweet_cnt = user['retweet_cnt']
        #     if int(user['follow_cnt']) > max_follow_cnt:
        #         max_follow_cnt = user['follow_cnt']
        #     if int(user['onehop_cnt']) > max_onehop_cnt:
        #         max_onehop_cnt = user['onehop_cnt']
        # spammer = self.sqlhelper.select_sql_one('SELECT uid FROM final_user WHERE spammer="yes"')
        # 先计算转发微博的关注比例
        for user in col.find():
            fre = 0
            # if user['retweet_cnt'] == 0:
            #     fre = float(follow_cnt) / retweet_cnt
            # else:
                # coefficient = math.log10(user['retweet_cnt'] + 1.0) / math.log10(max_retweet_cnt)
                # fre = coefficient * (float(user['follow_cnt']) / user['retweet_cnt']) + (1 - coefficient) * (
                # float(follow_cnt) / retweet_cnt)
            if float(user['retweet_cnt']) != 0:
                fre = str(float(user['follow_cnt']) / float(user['retweet_cnt']))
            if int(fre) == 0:
                pass
            col.update({'uid': user['uid']}, {'$set': {'follow_fre': fre}})
        # 再计算转发微博中一跳转发的比例
        for user in col.find():
            fre = 0
            # if user['retweet_cnt'] == 0:
            #     fre = float(onehop_cnt) / retweet_cnt
            # else:
            #     coefficient = math.log10(user['retweet_cnt'] + 1.0) / math.log10(max_retweet_cnt)
            #     fre = coefficient * (float(user['onehop_cnt']) / user['retweet_cnt']) + (1 - coefficient) * (
            #         float(onehop_cnt) / retweet_cnt)
            if float(user['retweet_cnt']) != 0:
                fre = str(float(user['onehop_cnt']) / float(user['retweet_cnt']))
            col.update({'uid': user['uid']}, {'$set': {'onehop_fre': fre}})

    @staticmethod
    def caculate_rvp_ratio(user, edge, edge_reverse):
        reciprocated_edge = 0
        edge_total_count = 0
        if user in edge.keys():
            edge_total_count += len(edge[user])
            for followee in edge[user]:
                if followee in edge_reverse.keys():
                    if user in edge_reverse[followee]:
                        reciprocated_edge += 1
        if user in edge_reverse.keys():
            edge_total_count += len(edge_reverse[user])

        if edge_total_count == 0:
            return 0.0
        return float(reciprocated_edge) / float(edge_total_count)

    def setFF(self):
        """
        :return: none
        """
        col = self.mdb.followCnt
        sqlhelper = SqlHelper()
        # spammer = sqlhelper.select_sql_one('SELECT uid FROM final_user WHERE spammer="yes"')
        # normal = sqlhelper.select_sql_one('SELECT uid FROM final_user WHERE spammer="no"')

        # cnt_dict = {}
        # profile = MongoClient().profile.json_text
        # for json_text in profile.find():
        #     uid = json_text['uid']
        #     if uid not in spammer and uid not in normal:
        #         continue
        #     cnt = 0
        #     try:
        #         for card in json_text['json_text']['cards']:
        #             try:
        #                 cnt += len(card['card_group'])
        #             except Exception as e:
        #                 pass
        #     except Exception as e:
        #         print('no cards %s' % uid)
        #     cnt_dict[uid] = cnt
        # for key in cnt_dict.keys():
        #     col.update({'uid': str(key)}, {'$set': {'profile': cnt_dict[key]}})
        #
        # followCnt = MongoClient().userFeature.followCnt
        # for user in followCnt.find():
        #     uid = user['uid']
        #     try:
        #         followee_cnt = followCnt.find_one({'uid': str(uid)})['followee_cnt']
        #         follower_cnt = followCnt.find_one({'uid': str(uid)})['follower_cnt']
        #         res = float(followee_cnt) / follower_cnt
        #         col.update({'uid': str(uid)}, {'$set': {'ff': res}})
        #     except Exception as e:
        #         print('no cards %s' % uid)

        uu = MongoClient().profile.user
        for user in uu.find():
            uid = user['uid']
            # if uid in spammer
            try:
                if uu.find_one({'uid': str(uid)})['json_text']['description'] != '':
                    col.update({'uid': str(uid)}, {'$set': {'description': 1}})
                else:
                    col.update({'uid': str(uid)}, {'$set': {'description': 0}})
            except Exception as e:
                print('no cards %s' % uid)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s : %(levelname)s  %(message)s',
                        datefmt='%Y-%m-%d %A %H:%M:%S')

    with UserFeature('localhost', 'sdh', 'root', 'root', 'utf8') as feature:
        feature.setRegisterDay()
        feature.setFollowCnt()
        feature.setRVP()
        feature.setOriThirdFre()
        feature.setRetweetFre()
        feature.setFF()
