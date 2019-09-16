# follower和followee的统计是在往数据库里写edge的时候就完成的，所以不要奇怪为什么这个类里没实现
import snownlp # 用于处理中文情感分析等任务
from pymongo import MongoClient

from src.util.SqlHelper import SqlHelper
from src.util.data_filter import write_dict_to_txt, calculate_cnt, write_dict_cnt_to_txt, init_dict
import time
from src.microblog.WblogFeature import WblogFeature
import jieba
import logging
class Statistics:
    def __init__(self):
        pass

    @staticmethod
    def forECDF(li):
        """
        将列表转换成能画累积分布图的dict
        :return:
        """
        res = {}
        for itm in li:
            if itm not in res.keys():
                res[itm] = 0
            res[itm] += 1
        return res

    @staticmethod
    def forECDF_record(di, filename):
        """
        将累积分布的dict写入文件中
        :param di:
        :return:
        """
        cnt = 0
        total = 0
        for key in di:
            total += di[key]
        res = sorted(di.items(), key=lambda x: x[0])
        with open(filename, 'w') as my_file:
            for itm in res:
                cnt += itm[1]
                my_file.write('%s %s\n' % (str(itm[0]), str(float(cnt) / total)))

    @staticmethod
    def count_wblog():
        sqlhelper = SqlHelper()

        wblog = {}

        for user in sqlhelper.select_sql_one('SELECT uid FROM user'):
            wblog[str(user)] = 0
            tmp = sqlhelper.select_cnt('SELECT count(*) FROM swblog WHERE uid=%s' % (str(user)))
            # print(tmp)
            if tmp:
                wblog[str(user)] += int(tmp)
            tmp = sqlhelper.select_cnt('SELECT count(*) FROM wblog WHERE uid=%s' % (str(user)))
            # print(tmp)
            if tmp:
                wblog[str(user)] += int(tmp)

        write_dict_to_txt(wblog, 'data\\wblog.txt')
        """
        1751565235 42
        5136420870 0
        3106192681 24
        3203825104 0
        2126474562 8
        2324752481 57
        """

        cnt = []
        for i in range(10000):
            cnt.append(i)
        wblog_cnt = init_dict(cnt, 0)

        calculate_cnt(wblog_cnt, wblog)

        write_dict_cnt_to_txt(wblog_cnt, 'data\\wblog_cnt.txt')
        """
        0 7938
        1 532
        2 336
        3 249
        4 189
        5 169
        6 151
        """

        sqlhelper.close()

    @staticmethod
    def count_comment():
        sqlhelper = SqlHelper()

        comment = {}
        col = MongoClient().wblog.wblog
        # i = 0
        for wblogId in sqlhelper.select_sql('SELECT wblogId FROM wblog'):
            wblogId = wblogId[0]
            cnt = 0
            try:
                wblog = col.find_one({'wblogId': str(wblogId)})['json_text']
                cnt = int(wblog['comments_count'])
                # print(cnt)
            except Exception as e:
                print(e)

            if cnt not in comment.keys():
                comment[cnt] = 1
            else:
                comment[cnt] += 1
            # i += 1
            # if i == 100:
            #     break
        # cnt = []
        # for i in range(10000):
        #     cnt.append(i)
        # comment_cnt = init_dict(cnt, 0)
        #
        # calculate_cnt(comment_cnt, comment)

        write_dict_cnt_to_txt(comment, 'data\\comment_cnt.txt')
        """
        0 615501
        1 120480
        2 74059
        3 47064
        4 37356
        5 29747
        6 25166
        """

        sqlhelper.close()

    @staticmethod
    def sort_comment():
        comment = {}
        sum = 0
        with open('data\\comment_cnt_bak.txt', 'r') as my_file:
            for line in my_file:
                line = line.split('\n')[0]
                num = line.split(' ')[0]
                cnt = line.split(' ')[1]
                comment[num] = cnt
                sum += int(num) * int(cnt)
        comment = sorted(comment.items(), key=lambda x: int(x[0]), reverse=False)
        print(sum)
        with open('data\\comment_cnt.txt', 'w') as my_file:
            for itm in comment:
                my_file.write(itm[0] + ' ' + itm[1] + '\n')

    @staticmethod
    def count_edge():
        sqlhelper = SqlHelper()

        cnt = 0

        spammer = sqlhelper.select_sql_one('SELECT uid FROM spammer')
        normal = sqlhelper.select_sql_one('SELECT uid FROM normal WHERE choose="yes"')
        for uid in spammer:
            if uid in normal:
                normal.remove(uid)
        all_user = spammer + normal
        print(len(all_user))
        for uid in all_user:
            for u in sqlhelper.select_sql('SELECT followeeUid FROM edge WHERE uid=%s' % str(uid)):
                if str(u[0]) in all_user:
                    cnt += 1
        print(cnt)

    @staticmethod
    def time_distribution():
        """
        统计时间周期分布
        :return:
        """
        sqlhelper = SqlHelper()
        res = {0: {},
               1: {},
               2: {},
               3: {},
               4: {},
               5: {},
               6: {},
               }
        for key in res.keys():
            for i in range(24):
                res[key][i] = 0

        for t in sqlhelper.select_sql('SELECT created_at FROM wblog'):
            timestamp = t[0]
            res[timestamp.weekday()][timestamp.hour] += 1

        with open('data/timestamp.txt', 'w') as my_file:
            for key in res.keys():
                for k in res[key].keys():
                    my_file.write(str(key * 24 + k) + ' ' + str(res[key][k]) + '\n')

    @staticmethod
    def sentiment():
        """
        为了画情感极性
        :return:
        """
        sqlhelper = SqlHelper()
        swblog = sqlhelper.select_sql_one('SELECT wblogId FROM swblog')
        wblog = sqlhelper.select_sql_one('SELECT wblogId FROM wblog_choose')

        final_wblog = sqlhelper.select_sql_one('SELECT wblogId FROM final_wblog WHERE spammer="yes"')
        for wblogId in final_wblog:
            if wblogId not in swblog:
                swblog.append(wblogId)

        for wblogId in swblog:
            if wblogId in wblog:
                wblog.remove(wblogId)

        all_wblog = swblog + wblog
        swblog_sentiment_dict = {}
        swblog_comment_cnt = 0
        wblog_sentiment_dict = {}
        wblog_comment_cnt = 0

        # 有一些评论很短或者没有字之类的
        # 对于这些微博，不参与计算情感极性
        # 过滤的方法是分词后判断去除一个词都不剩下的文本
        stop_words = WblogFeature.get_stop_words('stop_words.txt')

        cc = MongoClient().comment.comment

        for wblogId in all_wblog:
            corpus = []
            try:
                for comment in cc.find({'wblogId': str(wblogId)}):
                    text = WblogFeature.remove_html(comment['json_text']['text'])
                    text = WblogFeature.remove_tag(text)
                    fenci = list(jieba.cut_for_search(text))
                    if len(fenci) == 0:
                        continue
                    # 由于jieba分词没有提供去停用词的接口，所以手动去停用词
                    stop_cnt = 0
                    for word in fenci:
                        if word in stop_words:
                            stop_cnt += 1
                    if stop_cnt == len(fenci):
                        continue
                    corpus.append(text)
            except Exception as e:
                logging.error('%s. The wblogId is %s' % (e, str(wblogId)))
            if wblogId in swblog:
                swblog_comment_cnt += len(corpus)
                for text in corpus:
                    sen = round(float(snownlp.SnowNLP(text).sentiments), 1)
                    if sen not in swblog_sentiment_dict.keys():
                        swblog_sentiment_dict[sen] = 0
                    swblog_sentiment_dict[sen] += 1
            else:
                wblog_comment_cnt += len(corpus)
                for text in corpus:
                    sen = round(float(snownlp.SnowNLP(text).sentiments), 1)
                    if sen not in wblog_sentiment_dict.keys():
                        wblog_sentiment_dict[sen] = 0
                    wblog_sentiment_dict[sen] += 1

        with open('swblog_sentiment.txt', 'w') as my_file:
            for key in swblog_sentiment_dict.keys():
                my_file.write(str(key) + ' ' + str(float(swblog_sentiment_dict[key]) / swblog_comment_cnt) + '\n')
        with open('wblog_sentiment.txt', 'w') as my_file:
            for key in wblog_sentiment_dict.keys():
                my_file.write(str(key) + ' ' + str(float(wblog_sentiment_dict[key]) / wblog_comment_cnt) + '\n')

    @staticmethod
    def works():
        """
        统计众包水军参与任务次数
        :return:
        """
        sqlhelper = SqlHelper()
        w = {}
        for res in sqlhelper.select_sql('SELECT woUid FROM works1516'):
            woUid = res[0]
            if woUid not in w:
                w[woUid] = 0
            w[woUid] += 1
        w_cnt = {}
        for woUid in w.keys():
            cnt = w[woUid]
            if cnt not in w_cnt:
                w_cnt[cnt] = 1
            w_cnt[cnt] += 1
        w_cnt = sorted(w_cnt.items(), key=lambda x: x[0])
        with open('data/works.txt', 'w') as my_file:
            my_file.write('woUid cnt\n')
            for itm in w_cnt:
                my_file.write('%s %s\n' % (str(itm[0]), str(itm[1])))

    @staticmethod
    def profile_complete():
        """
        统计用户的主页信息完整程度
        :return:
        """
        sqlhelper = SqlHelper()
        spammer = sqlhelper.select_sql_one('SELECT uid FROM final_user WHERE spammer="yes"')
        normal = sqlhelper.select_sql_one('SELECT uid FROM final_user WHERE spammer="no"')

        cnt_dict = {}
        profile = MongoClient().profile.json_text
        for json_text in profile.find():
            uid = json_text['uid']
            if uid not in spammer and uid not in normal:
                continue
            cnt = 0
            try:
                for card in json_text['json_text']['cards']:
                    try:
                        cnt += len(card['card_group'])
                    except Exception as e:
                        pass
            except Exception as e:
                print('no cards %s' % uid)
            cnt_dict[uid] = cnt

        spammer_dict = {}
        spammer_cnt = 0
        normal_dict = {}
        normal_cnt = 0

        for key in cnt_dict.keys():
            if key in spammer:

                if cnt_dict[key] not in spammer_dict.keys():
                    spammer_dict[cnt_dict[key]] = 0
                spammer_dict[cnt_dict[key]] += 1
                spammer_cnt += 1
            else:
                if cnt_dict[key] not in normal_dict.keys():
                    normal_dict[cnt_dict[key]] = 0
                normal_dict[cnt_dict[key]] += 1
                normal_cnt += 1
        spammer_dict = sorted(spammer_dict.items(), key=lambda x: x[0])
        with open('data/profile_complete_spammer.txt', 'w') as my_file:
            cnt = 0
            for itm in spammer_dict:
                cnt += itm[1]
                my_file.write('%s %s\n' % (str(float(itm[0])), str(float(cnt) / spammer_cnt)))

        normal_dict = sorted(normal_dict.items(), key=lambda x: x[0])
        with open('data/profile_complete_normal.txt', 'w') as my_file:
            cnt = 0
            for itm in normal_dict:
                cnt += itm[1]
                my_file.write('%s %s\n' % (str(float(itm[0])), str(float(cnt) / normal_cnt)))

    @staticmethod
    def tongi():
        """
        各种统计
        :return:
        """
        sqlhelper = SqlHelper()
        spammer = sqlhelper.select_sql_one('SELECT uid FROM spammer')
        normal = sqlhelper.select_sql_one('SELECT uid FROM normal WHERE choose="yes"')

        final_user = sqlhelper.select_sql_one('SELECT uid FROM final_user WHERE spammer="yes"')
        for uid in final_user:
            if uid not in spammer:
                spammer.append(uid)

        for uid in spammer:
            if uid in normal:
                normal.remove(uid)
        print(len(spammer))
        print(len(normal))

        # followee = []
        # follower = []
        # followCnt = MongoClient().userFeature.followCnt
        # for uid in spammer:
        #     try:
        #         log_followee = followCnt.find_one({'uid': str(uid)})['log_followee']
        #         log_follower = followCnt.find_one({'uid': str(uid)})['log_follower']
        #         followee.append(float(log_followee))
        #         follower.append(float(log_follower))
        #     except Exception as e:
        #         print('%s---- %s' % (str(e), str(uid)))
        # followee = Statistics.forECDF(followee)
        # follower = Statistics.forECDF(follower)
        #
        # Statistics.forECDF_record(followee, 'ecdf/followee_spammer.txt')
        # Statistics.forECDF_record(follower, 'ecdf/follower_spammer.txt')

        # rvp = []
        # rvpm = MongoClient().userFeature.rvp
        # for uid in normal:
        #     try:
        #         rvp_ratio = rvpm.find_one({'uid': str(uid)})['rvp_ratio']
        #         rvp.append(float(rvp_ratio))
        #     except Exception as e:
        #         print('%s---- %s' % (str(e), str(uid)))
        #
        # rvp = Statistics.forECDF(rvp)
        #
        # Statistics.forECDF_record(rvp, 'ecdf/rvp_normal.txt')

        # fre = []
        # oriThirdFre = MongoClient().userFeature.oriThirdFre
        # for uid in normal:
        #     try:
        #         if str(oriThirdFre.find_one({'uid': str(uid)})['ori_cnt']) == '0':
        #             continue
        #         f = oriThirdFre.find_one({'uid': str(uid)})['fre_new']
        #         # if str(oriThirdFre.find_one({'uid': str(uid)})['thi_cnt']) == '0':
        #         #     f = 0
        #         fre.append(float(f))
        #     except Exception as e:
        #         print('%s---- %s' % (str(e), str(uid)))
        #
        # fre = Statistics.forECDF(fre)
        #
        # Statistics.forECDF_record(fre, 'ecdf/oriThirdFre_normal.txt')

        # follow_fre = []
        # onehop_fre = []
        # retweetFre = MongoClient().userFeature.retweetFre
        # for uid in spammer:
        #     try:
        #         ff = retweetFre.find_one({'uid': str(uid)})['follow_fre']
        #         of = retweetFre.find_one({'uid': str(uid)})['onehop_fre']
        #         if str(retweetFre.find_one({'uid': str(uid)})['retweet_cnt']) != '0':
        #             follow_fre.append(float(ff))
        #             onehop_fre.append(float(of))
        #     except Exception as e:
        #         print('%s---- %s' % (str(e), str(uid)))
        #
        # follow_fre = Statistics.forECDF(follow_fre)
        # onehop_fre = Statistics.forECDF(onehop_fre)
        #
        # Statistics.forECDF_record(follow_fre, 'ecdf/follow_fre_spammer.txt')
        # Statistics.forECDF_record(onehop_fre, 'ecdf/onehop_fre_spammer.txt')





    @staticmethod
    def interact():
        """
        统计微博评论的互动情况
        :return:
        """
        sqlhelper = SqlHelper()
        swblog = sqlhelper.select_sql_one('SELECT wblogId FROM swblog')
        wblog = sqlhelper.select_sql_one('SELECT wblogId FROM wblog_choose')

        final_wblog = sqlhelper.select_sql_one('SELECT wblogId FROM final_wblog WHERE spammer="yes"')
        for wblogId in final_wblog:
            if wblogId not in swblog:
                swblog.append(wblogId)

        for wblogId in swblog:
            if wblogId in wblog:
                wblog.remove(wblogId)
        print(len(swblog) + len(wblog))

        hot = 0
        interact = 0
        hotCommentRatio = MongoClient().wblogFeature.hotCommentRatio
        commentInteractRatio = MongoClient().wblogFeature.commentInteractRatio
        for wblogId in wblog:
            try:
                a = hotCommentRatio.find_one({'wblogId': str(wblogId)})['hot_ratio']
                b = commentInteractRatio.find_one({'wblogId': str(wblogId)})['interact_ratio']
                # if float(a) != 0:
                #     hot += 1
                # if float(b) != 0:
                #     interact += 1
                if float(a) != 0 or float(b) != 0:
                    hot += 1
            except Exception as e:
                print('%s---- %s' % (str(e), str(wblogId)))
        print()
        print(hot)
        print(len(wblog))
        print(float(hot) / len(wblog))
        print()
        hot=0
        for wblogId in swblog:
            try:
                a = hotCommentRatio.find_one({'wblogId': str(wblogId)})['hot_ratio']
                b = commentInteractRatio.find_one({'wblogId': str(wblogId)})['interact_ratio']
                if float(a) != 0 or float(b) != 0:
                    hot += 1
            except Exception as e:
                print('%s---- %s' % (str(e), str(wblogId)))

        print(hot)
        print(len(swblog))
        print(float(hot) / len(swblog))


if __name__ == '__main__':

    # Statistics.count_wblog()
    # Statistics.count_comment()
    # Statistics.sort_comment()
    # Statistics.count_edge()
    # Statistics.time_distribution()
    # Statistics.sentiment()
    # Statistics.works()
    # Statistics.profile_complete()
    # Statistics.tongi()
    Statistics.interact()
