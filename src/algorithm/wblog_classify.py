# 对微博进行分类
import numpy
from pymongo import MongoClient
# from sklearn.svm import SVC
# from sklearn import preprocessing
from sklearn.linear_model import LogisticRegression
import random
import logging
from sklearn import metrics
# import seaborn
# import matplotlib.pyplot as plt
# from sklearn.model_selection import cross_val_score
from src.algorithm.alkit import Alkit
from src.main.evaluation import Evaluation
from src.util.SqlHelper import SqlHelper
# from sklearn.ensemble import RandomForestClassifier
# from sklearn.ensemble import RandomForestRegressor
from collections import OrderedDict


class WblogClassify(object):

    def __init__(self, h, d, u, p, c, train_per=0.8, spam_per=0.1, reset_dataset=False, dump=True,
                 add_unknown_into_model=False,file_name_appendix=''):
        self.host = h
        self.db = d
        self.user = u
        self.passwd = p
        self.charset = c
        self.sqlhelper = SqlHelper(host=self.host, db=self.db, user=self.user, passwd=self.passwd, charset=self.charset)

        self.commentSimilarity = MongoClient().wblogFeature.commentSimilarity
        self.sentimentSimilarity = MongoClient().wblogFeature.sentimentSimilarity
        self.commentInteractRatio = MongoClient().wblogFeature.commentInteractRatio
        self.hotCommentRatio = MongoClient().wblogFeature.hotCommentRatio

        self.train_per = train_per
        self.spam_per = spam_per
        self.reset_dataset = reset_dataset
        self.dump = dump
        self.add_unknown_into_model = add_unknown_into_model

        self.file_name_appendix = file_name_appendix



    def run(self):
        """
        从数据库中读取特征数据，并使用svm和lr分类
        :return:
        """
        if not self.add_unknown_into_model:
            swblog = self.sqlhelper.select_sql_one('SELECT wblogId FROM swblog')
            wblog = self.sqlhelper.select_sql_one('SELECT wblogId FROM wblog_choose')

            final_wblog = self.sqlhelper.select_sql_one('SELECT wblogId FROM final_wblog WHERE spammer="yes"')
            for wblogId in final_wblog:
                if wblogId not in swblog:
                    swblog.append(wblogId)

            # 不知道为什么spammer和normal两个集合有重合的用户
            # 所以这里简单地将这些重合的用户都认为是spammer
            for uid in swblog:
                if uid in wblog:
                    wblog.remove(uid)

            """
            到目前为止，我们得到了下面几个有用的东西
            swblog： 水军  
            wblog： 正常用户
            unkonwn：还没来得及标注的未知类型微博
            """

            logging.info('原始数据spam占比例(max): %s' % (len(swblog) * 1.0 / (len(wblog) + len(swblog))))
            if self.spam_per > len(swblog) * 1.0 / (len(wblog) + len(swblog)):
                logging.info('we don\'t have so much spams in our datasets, we will keep original percentage')
            else:
                expected_spam_number = int(self.spam_per * len(wblog) * 1.0 / (1 - self.spam_per))
                swblog = random.sample(swblog, expected_spam_number)

            if self.reset_dataset:
                train_wblog_set = random.sample(swblog, int(len(swblog) * self.train_per)) + random.sample(wblog, int(
                    len(wblog) * self.train_per))
                test_wblog_set = list(set(swblog + wblog).difference(train_wblog_set))
                # # 第二期改进代码
                # train_user_set_without_unknown = random.sample(spammer, int(len(spammer) * train_per)) + random.sample(normal, int(len(normal) * train_per))
                # train_user_set_with_unknown = random.sample(spammer, int(len(spammer) * train_per)) + random.sample(normal, int(
                #     len(normal) * train_per))+random.sample(unknown, len(unknown))
                # test_user_set = list(set(spammer + normal).difference(train_user_set_without_unknown))
                # train_user_set=train_user_set_with_unknown+train_user_set_with_unknown
            else:
                train_wblog_set, test_wblog_set = Alkit.read_dataset(
                    '../main/prior/wblog_train' + self.file_name_appendix + '.txt',
                    '../main/prior/wblog_prior' + self.file_name_appendix + '.txt')

            # 输出训练集和测试集的一些信息
            logging.info('总数据集大小：%s' % (len(train_wblog_set)+len(test_wblog_set)))
            logging.info('训练集大小：%s' % len(train_wblog_set))
            logging.info('训练集中正例（swblog）大小：%s' % len(list(set(train_wblog_set).intersection(set(swblog)))))
            logging.info('训练集中负例（wblog）大小：%s' % len(list(set(train_wblog_set).intersection(set(wblog)))))
            logging.info('测试集大小：%s' % len(test_wblog_set))
            logging.info('测试集中正例（swblog）大小：%s' % len(list(set(test_wblog_set).intersection(set(swblog)))))
            logging.info('测试集中负例（wblog）大小：%s' % len(list(set(test_wblog_set).intersection(set(wblog)))))
        else:
            raise ('we will implement this later.')

        # 将训练集和测试集从数据库中读出来，以顺序字典存储（调用vlues()输出的list顺序和插入顺序一致）
        feature_dict_data, result_dict_data = self.load_data(train_wblog_set, swblog, wblog)
        train_feature, train_result = Alkit.process_data(feature_dict_data, result_dict_data)
        logging.info('训练集数据处理完毕')
        feature_dict_data, result_dict_data = self.load_data(test_wblog_set, swblog, wblog)
        test_feature, test_result = Alkit.process_data(feature_dict_data, result_dict_data)
        logging.info('测试集数据处理完毕')

        # 使用svm训练并输出结果
        # logging.info('\nSVM开始训练')
        # model = SVC(class_weight='balanced')
        # model.fit(train_feature, train_result)
        # logging.info('训练结束')
        # predict_result = model.predict(test_feature)
        # logging.info('准确率：%s' % metrics.precision_score(test_result, predict_result))
        # logging.info('召回率：%s' % metrics.recall_score(test_result, predict_result))
        # logging.info('F1：%s' % metrics.f1_score(test_result, predict_result))

        # 使用LR训练并输出结果
        logging.info('LR开始训练')
        model = LogisticRegression(class_weight='balanced')
        model.fit(train_feature, train_result)
        logging.info('训练结束')
        predict_result = model.predict(test_feature)
        logging.info('准确率：%s' % metrics.precision_score(test_result, predict_result))
        logging.info('召回率：%s' % metrics.recall_score(test_result, predict_result))
        logging.info('F1：%s' % metrics.f1_score(test_result, predict_result))

        # 使用LR输出概率形式的结果
        predict_result_proba = model.predict_proba(test_feature)
        prp = []
        for prob in predict_result_proba:
            prp.append(float(prob[0]) * -1 + float(prob[1]) * 1)

        # 将LR跑出来的两种结果保存下来，供下一步使用
        if self.dump:
            logging.info("保存结果输出到 " + '../main/prior/wblog_train' + self.file_name_appendix + '.txt'
                         + "和" + '../main/prior/wblog_prior' + self.file_name_appendix + '.txt')
            Alkit.write_prior('../main/prior/wblog_train' + self.file_name_appendix + '.txt',
                              '../main/prior/wblog_prior' + self.file_name_appendix + '.txt',
                              train_wblog_set, train_result, test_wblog_set, test_result, predict_result, prp)

        # 使用Random Forest训练并输出结果
        # logging.info('\nRandom Forest开始训练')
        # model = RandomForestClassifier(n_estimators=100, class_weight='balanced')
        # model.fit(train_feature, train_result)
        # logging.info('训练结束')
        #
        # importances = model.feature_importances_
        # print(importances)
        #
        # predict_result = model.predict(test_feature)
        # logging.info('准确率：%s' % metrics.precision_score(test_result, predict_result))
        # logging.info('召回率：%s' % metrics.recall_score(test_result, predict_result))
        # logging.info('F1：%s' % metrics.f1_score(test_result, predict_result))
        # 使用RF输出概率形式的结果
        # predict_result_proba = model.predict_proba(test_feature)
        # prp = []
        # for prob in predict_result_proba:
        #     prp.append(float(prob[0]) * -1 + float(prob[1]) * 1)
        # # 将RF跑出来的两种结果保存下来，供下一步使用
        # Alkit.write_prior('prior/wblog_train.txt', 'prior/wblog_prior.txt',
        #                   train_wblog_set, train_result, test_wblog_set, test_result, predict_result, prp)
        # return float(metrics.f1_score(test_result, predict_result))

        # feature_name = ['log_time', 'log_follower', 'log_followee', 'fre-re', 'fre', 'follow_fre', 'onehop_fre', 'rvp_ratio']
        # df = DataFrame(numpy.hstack((test_feature, test_result[:, None])),
        #                columns=feature_name + ["class"])
        # _ = seaborn.pairplot(df, vars=feature_name, hue="class", size=1.5)
        # plt.show()

        # feature_dict_data, result_dict_data = self.load_data(train_wblog_set + test_wblog_set, swblog, wblog)
        # test_feature, test_result = Alkit.process_data(feature_dict_data, result_dict_data)
        # logging.info('数据处理完毕')
        #
        # logging.info('\nSVM开始训练-交叉验证')
        # model = SVC(class_weight='balanced')
        # res = cross_val_score(model, test_feature, test_result, cv=5, scoring='f1')
        # logging.info('训练结束')
        # logging.info(res)
        #
        # logging.info('\nLR开始训练-交叉验证')
        # model = LogisticRegression(class_weight='balanced')
        # res = cross_val_score(model, test_feature, test_result, cv=5, scoring='f1')
        # logging.info('训练结束')
        # logging.info(res)

    def evalutaion(self):
        """
        评价一下
        :return:
        """
        wblog_train_dict, wblog_train_list, wblog_prior_dict, wblog_prior_list = \
            Alkit.read_prior('../main/prior/wblog_train' + self.file_name_appendix + '.txt', '../main/prior/wblog_prior' + self.file_name_appendix + '.txt')
        swblog, swblog_prior, nwblog, nwblog_prior = Alkit.setSN(wblog_train_dict, wblog_prior_dict)
        scores = []
        test_result = []
        predict_result = []
        for uid in wblog_prior_list:
            test_result.append(float(wblog_prior_dict[uid]['label']))
            predict_result.append(float(wblog_prior_dict[uid]['prior_label']))
            scores.append(float(wblog_prior_dict[uid]['prior']))
        # print(float(metrics.f1_score(test_result, predict_result)))
        Evaluation.evaluation_self(scores, test_result)

        # ap
        p, r, thresholds = metrics.precision_recall_curve(test_result, scores)
        ap = metrics.average_precision_score(test_result, scores)
        logging.info('wblog AP:%s' % str(ap))
        with open('../main/lr/wblog_ap'+self.file_name_appendix+'.txt', 'w') as my_file:
            my_file.write('p r\n')
            for i in range(len(p)):
                my_file.write('%s %s\n' % (str(p[i]), str(r[i])))

        # roc
        fpr, tpr, thresholds = metrics.roc_curve(test_result, scores)
        logging.info('wblog AUC:%s' % str(metrics.auc(fpr, tpr)))
        with open('../main/lr/wblog_roc'+self.file_name_appendix+'.txt', 'w') as my_file:
            my_file.write('fpr tpr\n')
            for i in range(len(fpr)):
                my_file.write('%s %s\n' % (str(fpr[i]), str(tpr[i])))

        # top k precision
        wblog_score = {}
        for i in range(len(scores)):
            wblog_score[wblog_prior_list[i]] = scores[i]
        wblog_score = sorted(wblog_score.items(), key=lambda im: float(im[1]), reverse=True)
        with open('../main/lr/res_wblog_top'+self.file_name_appendix+'.txt', 'w') as my_file:
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

    def load_data(self, total_set, swblog, wblog, unknown=None):
        """
        从数据库读取数据，因为训练集和测试集读取的操作一样，所以单独写一个方法
        :return: 特征字典数据，类别字典数据
        """
        feature_dict_data = OrderedDict()
        result_dict_data = OrderedDict()

        for wblogId in total_set:
            feature_dict_data[wblogId] = [Alkit.load_data_help_w(self.commentSimilarity, wblogId, 'comment_similarity'),
                                          Alkit.load_data_help_w(self.sentimentSimilarity, wblogId,
                                                                 'sentiment_similarity'),
                                          Alkit.load_data_help_w(self.commentInteractRatio, wblogId, 'interact_ratio'),
                                          Alkit.load_data_help_w(self.hotCommentRatio, wblogId, 'hot_ratio')]

            # feature_dict_data[wblogId] = [Alkit.load_data_help_w(self.commentSimilarity, wblogId, 'comment_similarity'),
            #                               Alkit.load_data_help_w(self.commentInteractRatio, wblogId, 'interact_ratio'),
            #                               Alkit.load_data_help_w(self.hotCommentRatio, wblogId, 'hot_ratio')]

            if wblogId in swblog:
                result_dict_data[wblogId] = 1
            elif wblogId in wblog:
                result_dict_data[wblogId] = -1
            elif wblogId in unknown:
                result_dict_data[wblogId] = 0

        return feature_dict_data, result_dict_data


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s : %(levelname)s  %(message)s',
                        datefmt='%Y-%m-%d %A %H:%M:%S')

    blog_class = WblogClassify('localhost', 'sdh', 'root', 'root', 'utf8')
    blog_class.run(train_per=0.8, spam_per=0.9, reset_dataset=True, dump=True)
    """
    2%, 4%, 6%, 8%, 10%, 12%, 14%, 16%, 18%, 20%, 22%, 24%, 26%
    """
    print("start to evaluation")
    blog_class.evalutaion()
