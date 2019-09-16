# 跑crowd_target算法
import logging
from collections import OrderedDict
from sklearn import metrics

import numpy
import pymongo
from pymongo import MongoClient
from sklearn.ensemble import AdaBoostClassifier
from sklearn.tree import DecisionTreeClassifier

from src.algorithm.alkit import Alkit
from src.main.evaluation import Evaluation
from src.util.SqlHelper import SqlHelper
import time
from scipy import stats


class CrowdTarget():

    def __init__(self, h, d, u, p, c, file_name_appendix=''):
        """
        在init中将读取CrowdTarget必要的数据
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
        self.all_wblog = self.wblog_train_list + self.wblog_prior_list

        self.mdb = MongoClient().crowd_target       # 代码原来是crowd_target，因为我数据库的名字写错了所以改成crow_target
        self.sqlhelper = SqlHelper()

    def feature_retweet_time(self):
        col = self.mdb.time
        if not col.find_one():
            logging.info('retweet_time为空，设置主键为wblogId')
            col.create_index([('wblogId', pymongo.DESCENDING)], unique=True)

        cc = MongoClient().comment.comment
        for wblogId in self.all_wblog:

            if wblogId in self.swblog:
                col.insert_one({'wblogId': wblogId, 'spammer': 'true'})
            else:
                col.insert_one({'wblogId': wblogId, 'spammer': 'false'})

            t = self.sqlhelper.select_sql_one('SELECT created_at FROM wblog WHERE wblogId=%s' % str(wblogId))
            if not t:
                t = self.sqlhelper.select_sql_one('SELECT created_at FROM swblog WHERE wblogId=%s' % str(wblogId))
            a = time.mktime(time.strptime(t[0], '%Y/%m/%d %H:%M:%S'))
            res = 0.0
            cnt = 0
            time_list = []
            try:
                for comment in cc.find({'wblogId': str(wblogId)}):
                    created_at = comment['json_text']['created_at'] + ':00'
                    if len(created_at.split('-')[0]) != 4:
                        created_at = '2017-' + created_at
                    b = time.mktime(time.strptime(created_at, '%Y-%m-%d %H:%M:%S'))
                    res += b - a
                    cnt += 1
                    time_list.append(res)
            except Exception as e:
                logging.error('%s. The wblogId is %s' % (e, str(wblogId)))

            if cnt != 0:
                col.update({'wblogId': wblogId}, {'$set': {'mean': str(res / cnt)}})

            if cnt > 3:
                col.update({'wblogId': wblogId}, {'$set': {'std': str(numpy.std(numpy.array(time_list), ddof=1))}})
                col.update({'wblogId': wblogId}, {'$set': {'skewness': str(stats.skew(numpy.array(time_list)))}})
                col.update({'wblogId': wblogId}, {'$set': {'kurtosis': str(stats.kurtosis(numpy.array(time_list)))}})

        logging.info('feature_time finished')

    def feature_third(self):
        col = self.mdb.third
        if not col.find_one():
            logging.info('retweet_third为空，设置主键为wblogId')
            col.create_index([('wblogId', pymongo.DESCENDING)], unique=True)

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

        cc = MongoClient().comment.comment

        for wblogId in self.all_wblog:
            cnt = 0
            third_cnt = 0
            if wblogId in self.swblog:
                col.insert_one({'wblogId': wblogId, 'spammer': 'true'})
            else:
                col.insert_one({'wblogId': wblogId, 'spammer': 'false'})

            try:
                for comment in cc.find({'wblogId': str(wblogId)}):
                    source = comment['json_text']['source']
                    if source in third_party:
                        third_cnt += 1
                    cnt += 1
            except Exception as e:
                logging.error('%s. The wblogId is %s' % (e, str(wblogId)))

            if cnt > 1:
                if cnt != 0:
                    # if third_cnt != 0:
                    #     print(wblogId)
                    #     print(float(third_cnt) / cnt)
                    col.update({'wblogId': wblogId}, {'$set': {'third': str(float(third_cnt) / cnt)}})
        # for wblogId in self.all_wblog:
        #     retweet_list = []
        #     cnt = 0
        #     try:
        #         for wid in self.sqlhelper.select_sql('SELECT wblogId FROM wblog WHERE paMid=%s' % str(wblogId)):
        #             retweet_list.append(wid[0])
        #         for wid in self.sqlhelper.select_sql('SELECT wblogId FROM wblog WHERE orMid=%s' % str(wblogId)):
        #             if wid[0] not in retweet_list:
        #                 retweet_list.append(wid[0])
        #         # print(retweet_list)
        #         # print(len(retweet_list))
        #         for wid in retweet_list:
        #             res = self.sqlhelper.select_sql_one('SELECT source FROM wblog WHERE wblogId=%s' % str(wid))
        #             if len(res) == 0:
        #                 continue
        #             source = res[0]
        #             if source in third_party:
        #                 cnt += 1
        #     except Exception as e:
        #         logging.error('%s. The wblogId is %s' % (e, str(wblogId)))
        #
        #     if len(retweet_list) > 1:
        #         if cnt != 0:
        #             print(wblogId)
        #             print(float(cnt) / len(retweet_list))

    def feature_ur(self):
        col = self.mdb.ur
        if not col.find_one():
            logging.info('retweet_ur为空，设置主键为wblogId')
            col.create_index([('wblogId', pymongo.DESCENDING)], unique=True)

        total_user = []
        for uid in self.sqlhelper.select_sql('SELECT uid FROM spammer'):
            total_user.append(str(uid[0]))
        for uid in self.sqlhelper.select_sql('SELECT uid FROM normal'):
            if str(uid[0]) not in total_user:
                total_user.append(str(uid[0]))

        cc = MongoClient().comment.comment
        process_cnt = 0.0
        for wblogId in self.all_wblog:
            cnt = 0
            follow_cnt = 0
            if wblogId in self.swblog:
                col.insert_one({'wblogId': wblogId, 'spammer': 'true'})
            else:
                col.insert_one({'wblogId': wblogId, 'spammer': 'false'})

            poster_uid = self.sqlhelper.select_sql_first('SELECT uid FROM swblog WHERE wblogId=%s' % str(wblogId))
            if poster_uid == -1:
                poster_uid = self.sqlhelper.select_sql_first('SELECT uid FROM wblog WHERE wblogId=%s' % str(wblogId))

            try:
                for comment in cc.find({'wblogId': str(wblogId)}):
                    uid = comment['json_text']['user']['id']
                    if str(uid) in total_user:
                        cnt += 1
                        for followeeUid in self.sqlhelper.select_sql(
                                'SELECT followeeUid FROM edge1516 WHERE uid=%s' % str(uid)):
                            if str(followeeUid[0]) == str(poster_uid):
                                follow_cnt += 1
                                break
            except Exception as e:
                logging.error('%s. The wblogId is %s' % (e, str(wblogId)))

            process_cnt += 1.0
            print('processing:%s' % str(process_cnt / len(self.all_wblog)))

            if cnt > 1:
                if cnt != 0:
                    # if follow_cnt != 0:
                    #     print(wblogId)
                    #     print(float(follow_cnt) / cnt)
                    col.update({'wblogId': wblogId}, {'$set': {'ur': str(float(follow_cnt) / cnt)}})

    def feature_click(self):
        col = self.mdb.click
        if not col.find_one():
            logging.info('click为空，设置主键为wblogId')
            col.create_index([('wblogId', pymongo.DESCENDING)], unique=True)

        ws = MongoClient().wblog.swblog
        ww = MongoClient().wblog.wblog
        for wblogId in self.all_wblog:
            if wblogId in self.swblog:
                pass
            else:
                wblog = ww.find_one({'wblogId': str(wblogId)})
                content = wblog['json_text']['text']
                if 'ttarticle' in content:
                    print('https:' + content.split('ttarticle')[0].split(':')[-1] + 'ttarticle' +
                          content.split('ttarticle')[1].split('&')[0])

        for wblog in ws.find():
            content = wblog['json_text']['text']
            if 'ttarticle' in content:
                print('https:' + content.split('ttarticle')[0].split(':')[-1] + 'ttarticle' +
                      content.split('ttarticle')[1].split('&')[0])

    def run(self, train_per=0.8, reset_dataset=False):
        """
        从数据库中读取特征数据，并使用adaboost分类
        :return:
        """
        # 首先划分训练集微博和测试集微博
        swblog = self.sqlhelper.select_sql_one('SELECT wblogId FROM swblog')
        wblog = self.sqlhelper.select_sql_one('SELECT wblogId FROM wblog_choose')

        final_wblog = self.sqlhelper.select_sql_one('SELECT wblogId FROM final_wblog WHERE spammer="yes"')
        for wblogId in final_wblog:
            if wblogId not in swblog:
                swblog.append(wblogId)

        for uid in swblog:
            if uid in wblog:
                wblog.remove(uid)

        train_wblog_set, test_wblog_set = Alkit.read_dataset(
            '../main/prior/wblog_train' + self.file_name_appendix + '.txt',
            '../main/prior/wblog_prior' + self.file_name_appendix + '.txt')

        # 输出训练集和测试集的一些信息
        logging.info('训练集大小：%s' % len(train_wblog_set))
        logging.info('训练集中正例（swblog）大小：%s' % len(list(set(train_wblog_set).intersection(set(swblog)))))
        logging.info('训练集中负例（wblog）大小：%s' % len(list(set(train_wblog_set).intersection(set(wblog)))))
        logging.info('测试集大小：%s' % len(test_wblog_set))
        logging.info('测试集中正例（swblog）大小：%s' % len(list(set(test_wblog_set).intersection(set(swblog)))))
        logging.info('测试集中负例（wblog）大小：%s' % len(list(set(test_wblog_set).intersection(set(wblog)))))

        # print('279 train_wblog_set \n', train_wblog_set)
        # print('279 swblog \n', swblog)
        # print('279 wblog \n', wblog)

        # 将训练集和测试集从数据库中读出来，以顺序字典存储（调用vlues()输出的list顺序和插入顺序一致）
        feature_dict_data, result_dict_data = self.load_data(train_wblog_set, swblog, wblog)
        # print('281 feature_dict_data ', feature_dict_data)  # [('4033482998743585', [nan, nan, nan, nan, nan]),
        # print('282 result_dict_data', result_dict_data)  # [('4033482998743585', 1), ('3914608449995325', 1),

        train_feature, train_result = Alkit.process_data(feature_dict_data, result_dict_data)
        logging.info('训练集数据处理完毕')
        feature_dict_data, result_dict_data = self.load_data(test_wblog_set, swblog, wblog)
        test_feature, test_result = Alkit.process_data(feature_dict_data, result_dict_data)
        logging.info('测试集数据处理完毕')

        # 使用ad-boost训练并输出结果
        logging.info('\nAdaBoost开始训练')
        model = AdaBoostClassifier(DecisionTreeClassifier(max_depth=2, min_samples_split=20, min_samples_leaf=5),
                                   algorithm="SAMME",
                                   n_estimators=100, learning_rate=0.5)
        model.fit(train_feature, train_result)
        logging.info('训练结束')
        predict_result = model.predict(test_feature)
        logging.info('准确率：%s' % metrics.precision_score(test_result, predict_result))
        logging.info('召回率：%s' % metrics.recall_score(test_result, predict_result))
        logging.info('F1：%s' % metrics.f1_score(test_result, predict_result))
        predict_result_proba = model.predict_proba(test_feature)
        prp = []
        for prob in predict_result_proba:
            prp.append(float(prob[0]) * -1 + float(prob[1]) * 1)
        Alkit.write_prior('../main/crowd_target/wblog_train' + self.file_name_appendix + '.txt',
                          '../main/crowd_target/wblog_prior' + self.file_name_appendix + '.txt',
                          train_wblog_set, train_result, test_wblog_set, test_result, predict_result, prp)

    def evalutaion(self):
        """
        评价一下
        :return:
        """
        wblog_train_dict, wblog_train_list, wblog_prior_dict, wblog_prior_list = \
            Alkit.read_prior('../main/crowd_target/wblog_train' + self.file_name_appendix + '.txt',
                             '../main/crowd_target/wblog_prior' + self.file_name_appendix + '.txt')
        swblog, swblog_prior, nwblog, nwblog_prior = Alkit.setSN(wblog_train_dict, wblog_prior_dict)
        scores = []
        test_result = []
        predict_result = []
        for uid in wblog_prior_list:
            test_result.append(float(wblog_prior_dict[uid]['label']))
            predict_result.append(float(wblog_prior_dict[uid]['prior_label']))
            scores.append(float(wblog_prior_dict[uid]['prior']))
        Evaluation.evaluation_self(scores, test_result)

        # ap
        p, r, thresholds = metrics.precision_recall_curve(test_result, scores)
        ap = metrics.average_precision_score(test_result, scores)
        logging.info('wblog AP:%s' % str(ap))
        with open('../main/crowd_target/wblog_ap' + self.file_name_appendix + '.txt', 'w') as my_file:
            my_file.write('p r\n')
            for i in range(len(p)):
                my_file.write('%s %s\n' % (str(p[i]), str(r[i])))

        # roc
        fpr, tpr, thresholds = metrics.roc_curve(test_result, scores)
        logging.info('wblog AUC:%s' % str(metrics.auc(fpr, tpr)))
        with open('../main/crowd_target/wblog_roc' + self.file_name_appendix + '.txt', 'w') as my_file:
            my_file.write('fpr tpr\n')
            for i in range(len(fpr)):
                my_file.write('%s %s\n' % (str(fpr[i]), str(tpr[i])))

        # top k precision
        wblog_score = {}
        for i in range(len(scores)):
            wblog_score[wblog_prior_list[i]] = scores[i]
        wblog_score = sorted(wblog_score.items(), key=lambda im: float(im[1]), reverse=True)
        with open('../main/crowd_target/res_wblog_top' + self.file_name_appendix + '.txt', 'w') as my_file:
            my_file.write('type wblogId score precision top_k\n')
            wblog_count_now = 0
            top_k = 0
            for itm in wblog_score:
                uid = itm[0]
                score = itm[1]
                if uid in swblog:
                    u_type = 's'
                    wblog_count_now += 1
                else:
                    u_type = 'n'
                top_k += 1
                precision = str(float(wblog_count_now) / top_k)
                my_file.write(u_type + ' ' + str(uid) + ' ' + str(score) + ' ' + precision + ' ' + str(top_k) + '\n')


    def load_data(self, total_set, swblog, wblog):
        """
        从数据库读取数据，因为训练集和测试集读取的操作一样，所以单独写一个方法
        :return: 特征字典数据，类别字典数据
        total_set=train_wblog_set, ['4033482998743585', '3914608449995325',
        swblog=swblog, ['4045047554826553', '4039829169862097',
        wblog=wblog, ['4032096583879003', '4054839190956692',
        """
        feature_dict_data = OrderedDict()
        result_dict_data = OrderedDict()

        for wblogId in total_set:
            feature_dict_data[wblogId] = [Alkit.load_data_help_w(self.mdb.time, wblogId, 'mean'),
                                          Alkit.load_data_help_w(self.mdb.time, wblogId, 'std'),
                                          Alkit.load_data_help_w(self.mdb.time, wblogId, 'skewness'),
                                          Alkit.load_data_help_w(self.mdb.time, wblogId, 'kurtosis'),
                                          Alkit.load_data_help_w(self.mdb.third, wblogId, 'third')]

            if wblogId in swblog:
                result_dict_data[wblogId] = 1
            else:
                result_dict_data[wblogId] = -1

        # print("388 feature_dict_data\n", feature_dict_data)

        return feature_dict_data, result_dict_data


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s : %(levelname)s  %(message)s',
                        datefmt='%Y-%m-%d %A %H:%M:%S')

    # ct = CrowdTarget('localhost', 'sdh', 'root', 'root', 'utf8')
    # ct.feature_mean()
    a = [1, 3, 4, 5, 8, 11]
    print(stats.skew(a))
    print(stats.kurtosis(a))
