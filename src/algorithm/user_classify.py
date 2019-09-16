# 对user进行分类
import numpy
from pandas import DataFrame
from pymongo import MongoClient
from sklearn.svm import SVC
from sklearn import preprocessing
from sklearn.linear_model import LogisticRegression
import random
import logging
from sklearn import metrics
import seaborn
import matplotlib.pyplot as plt
from sklearn.model_selection import cross_val_score
from src.algorithm.alkit import Alkit
from src.main.evaluation import Evaluation
from src.util.SqlHelper import SqlHelper
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import RandomForestRegressor
from collections import OrderedDict


class UserClassify(object):

    def __init__(self, h, d, u, p, c, train_per=0.8, spammer_per=0.1, reset_dataset=False, dump=True,
                 add_unknown_into_model=False, file_name_appendix=''):
        self.host = h
        self.db = d
        self.user = u
        self.passwd = p
        self.charset = c
        self.sqlhelper = SqlHelper(host=self.host, db=self.db, user=self.user, passwd=self.passwd, charset=self.charset)

        self.registerDay = MongoClient().userFeature.registerDay
        self.followCnt = MongoClient().userFeature.followCnt
        self.oriThirdFre = MongoClient().userFeature.oriThirdFre
        self.retweetFre = MongoClient().userFeature.retweetFre
        self.rvp = MongoClient().userFeature.rvp

        self.train_per = train_per
        self.spammer_per = spammer_per
        self.reset_dataset = reset_dataset
        self.dump = dump
        self.add_unknown_into_model = add_unknown_into_model
        self.file_name_appendix = file_name_appendix

    def run(self):
        """
        从数据库中读取特征数据，并使用svm和lr分类

        水军占比例(max): 0.2325521503991759
        spammer_per <= 0.2325521503991759



        :return:
        """

        if not self.add_unknown_into_model:
            # 首先划分训练集用户和测试集用户
            spammer = self.sqlhelper.select_sql_one('SELECT uid FROM spammer')
            normal = self.sqlhelper.select_sql_one('SELECT uid FROM normal WHERE choose="yes"')
            # unknown = self.sqlhelper.select_sql_one('SELECT uid FROM normal WHERE choose="not"')

            final_user = self.sqlhelper.select_sql_one('SELECT uid FROM final_user WHERE spammer="yes"')

            """
            final_user: 3843个用户， 水军903， 非水军2940
            normal： 13906个用户， 水军和非水军未知，为此我们通过人工的方法从从这些用户中挑选了一些正常的用户，标记为choose='yes'
            spammer: 892个水军用户
    
            """
            for uid in final_user:
                if uid not in spammer:
                    spammer.append(uid)

            """
            到这为止, 代码中spammer相当于数据表里spammer U final_user.spammer一共有903
            """

            # 不知道为什么spammer和normal两个集合有重合的用户
            # 所以这里简单地将这些重合的用户都认为是spammer
            for uid in spammer:
                if uid in normal:
                    normal.remove(uid)
                # if uid in unknown:
                #     unknown.remove(uid)

            """
            到目前为止，我们得到了下面几个有用的东西
            spammer： 水军  
            normal： 正常用户
            unkonwn：还没来得及标注的未知类型用户
            """
            logging.info('原始数据水军占比例(max): %s' % (len(spammer) * 1.0 / (len(normal) + len(spammer))))
            if self.spammer_per > len(spammer) * 1.0 / (len(normal) + len(spammer)):
                logging.info('we don\'t have so much spammers in our datasets, we will keep original percentage')
            else:
                expected_spammer_number = int(self.spammer_per * len(normal) * 1.0 / (1 - self.spammer_per))
                spammer = random.sample(spammer, expected_spammer_number)

            # print(len(spammer))
            if self.reset_dataset:
                train_user_set = random.sample(spammer, int(len(spammer) * self.train_per)) + random.sample(
                    normal, int(len(normal) * self.train_per))
                test_user_set = list(set(spammer + normal).difference(train_user_set))
                # # 第二期改进代码
                # train_user_set_without_unknown = random.sample(spammer, int(len(spammer) * train_per)) + random.sample(normal, int(len(normal) * train_per))
                # train_user_set_with_unknown = random.sample(spammer, int(len(spammer) * train_per)) + random.sample(normal, int(
                #     len(normal) * train_per))+random.sample(unknown, len(unknown))
                # test_user_set = list(set(spammer + normal).difference(train_user_set_without_unknown))
                # train_user_set=train_user_set_with_unknown+train_user_set_with_unknown
            else:
                train_user_set, test_user_set = Alkit.read_dataset(
                    '../main/prior/user_train' + self.file_name_appendix + '.txt',
                    '../main/prior/user_prior' + self.file_name_appendix + '.txt')

            # 输出训练集和测试集的一些信息
            logging.info('数据集总大小：%s' % (len(train_user_set) + len(test_user_set)))
            logging.info('训练集大小：%s' % len(train_user_set))
            logging.info('训练集中正例（spammer）大小：%s' % len(list(set(train_user_set).intersection(set(spammer)))))
            logging.info('训练集中负例（normal）大小：%s' % len(list(set(train_user_set).intersection(set(normal)))))
            # logging.info('训练集中未知标签（unknown）大小：%s' % len(list(set(unknown))))
            logging.info('测试集大小：%s' % len(test_user_set))
            logging.info('测试集中正例（spammer）大小：%s' % len(list(set(test_user_set).intersection(set(spammer)))))
            logging.info('测试集中负例（normal）大小：%s' % len(list(set(test_user_set).intersection(set(normal)))))
            logging.info('水军占比例: %s' % (len(spammer) * 1.0 / (len(normal) + len(spammer))))
            """
            测试集参与训练，但是测试集在模型训练期间标签将按照unknown处理
            """
        else:
            raise ('we will implement this later.')

        # 将训练集和测试集从数据库中读出来，以顺序字典存储（调用vlues()输出的list顺序和插入顺序一致）
        feature_dict_data, result_dict_data = self.load_data(train_user_set, spammer, normal)
        train_feature, train_result = Alkit.process_data(feature_dict_data, result_dict_data)
        logging.info('训练集数据处理完毕')
        feature_dict_data, result_dict_data = self.load_data(test_user_set, spammer, normal)
        test_feature, test_result = Alkit.process_data(feature_dict_data, result_dict_data)
        logging.info('测试集数据处理完毕')
        # print(metrics.mutual_info_score(train_result, train_feature))
        # 使用svm训练并输出结果
        # logging.info('\nSVM开始训练')
        # model = SVC(class_weight='balanced')
        # model.fit(train_feature, train_result)
        # logging.info('训练结束')
        # predict_result = model.predict(test_feature)
        # logging.info('准确率：%s' % metrics.precision_score(test_result, predict_result))
        # logging.info('召回率：%s' % metrics.recall_score(test_result, predict_result))
        # logging.info('F1：%s' % metrics.f1_score(test_result, predict_result))

        # import minepy
        # m = minepy.MINE()
        # for i in range(7):
        #     m.compute_score(train_feature[:,i], train_result)
        #     print(m.mic())

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
            logging.info("保存结果输出到 " +
                         '../main/prior/user_train' + self.file_name_appendix + '.txt 和' +
                         '../main/prior/user_prior' + self.file_name_appendix + '.txt')
            Alkit.write_prior('../main/prior/user_train' + self.file_name_appendix + '.txt',
                              '../main/prior/user_prior' + self.file_name_appendix + '.txt',
                              train_user_set, train_result, test_user_set, test_result, predict_result, prp)

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
        # Alkit.write_prior('prior/user_train.txt', 'prior/user_prior.txt',
        #                   train_user_set, train_result, test_user_set, test_result, predict_result, prp)
        # return float(metrics.f1_score(test_result, predict_result))

        # feature_name = ['log_time', 'log_follower', 'log_followee', 'fre-re', 'fre', 'follow_fre', 'onehop_fre', 'rvp_ratio']
        # df = DataFrame(numpy.hstack((test_feature, test_result[:, None])),
        #                columns=feature_name + ["class"])
        # _ = seaborn.pairplot(df, vars=feature_name, hue="class", size=1.5)
        # plt.show()

        # feature_dict_data, result_dict_data = self.load_data(train_user_set + test_user_set, spammer, normal)
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
        user_train_dict, user_train_list, user_prior_dict, user_prior_list = \
            Alkit.read_prior('../main/prior/user_train' + self.file_name_appendix + '.txt',
                             '../main/prior/user_prior' + self.file_name_appendix + '.txt')

        spammer, spammer_prior, normal, normal_prior = Alkit.setSN(user_train_dict, user_prior_dict)
        scores = []
        test_result = []
        predict_result = []
        for uid in user_prior_list:
            test_result.append(float(user_prior_dict[uid]['label']))
            predict_result.append(float(user_prior_dict[uid]['prior_label']))
            scores.append(float(user_prior_dict[uid]['prior']))
        # print(float(metrics.f1_score(test_result, predict_result)))
        Evaluation.evaluation_self(scores, test_result)

        # ap
        p, r, thresholds = metrics.precision_recall_curve(test_result, scores)
        ap = metrics.average_precision_score(test_result, scores)
        logging.info('user AP:%s' % str(ap))
        with open('../main/lr/user_ap' + self.file_name_appendix + '.txt', 'w') as my_file:
            my_file.write('p r\n')
            for i in range(len(p)):
                my_file.write('%s %s\n' % (str(p[i]), str(r[i])))

        # roc
        fpr, tpr, thresholds = metrics.roc_curve(test_result, scores)
        logging.info('user AUC:%s' % str(metrics.auc(fpr, tpr)))
        with open('../main/lr/user_roc' + self.file_name_appendix + '.txt', 'w') as my_file:
            my_file.write('fpr tpr\n')
            for i in range(len(fpr)):
                my_file.write('%s %s\n' % (str(fpr[i]), str(tpr[i])))

        # top k precision
        worker_score = {}
        for i in range(len(scores)):
            worker_score[user_prior_list[i]] = scores[i]
        worker_score = sorted(worker_score.items(), key=lambda im: float(im[1]), reverse=True)
        with open('../main/lr/res_user_top' + self.file_name_appendix + '.txt', 'w') as my_file:
            my_file.write('type uid score precision top_k\n')
            worker_count_now = 0
            top_k = 0
            for itm in worker_score:
                uid = itm[0]
                score = itm[1]
                if uid in spammer:
                    u_type = 'w'
                    worker_count_now += 1
                else:
                    u_type = 'n'
                top_k += 1
                precision = str(float(worker_count_now) / top_k)
                my_file.write(u_type + ' ' + str(uid) + ' ' + str(score) + ' ' + precision + ' ' + str(top_k) + '\n')

    def load_data(self, total_set, spammer, normal, unknown=None):
        """
        从数据库读取数据，因为训练集和测试集读取的操作一样，所以单独写一个方法
        :return: 特征字典数据，类别字典数据
        """
        feature_dict_data = OrderedDict()
        result_dict_data = OrderedDict()

        for uid in total_set:
            feature_dict_data[uid] = [Alkit.load_data_help(self.registerDay, uid, 'log_time'),
                                      Alkit.load_data_help(self.followCnt, uid, 'log_follower'),
                                      Alkit.load_data_help(self.followCnt, uid, 'log_followee'),
                                      Alkit.load_data_help(self.oriThirdFre, uid, 'fre'),
                                      Alkit.load_data_help(self.retweetFre, uid, 'follow_fre'),
                                      Alkit.load_data_help(self.retweetFre, uid, 'onehop_fre'),
                                      Alkit.load_data_help(self.rvp, uid, 'rvp_ratio')]

            """
            现在我需要检查一下， 看看mongodb里这些json数据表是不是仅仅包含了normal和spammer而没有把unknown放进来？
            
             self.registerDay = MongoClient().userFeature.registerDay
                self.followCnt = MongoClient().userFeature.followCnt
                self.oriThirdFre = MongoClient().userFeature.oriThirdFre
                self.retweetFre = MongoClient().userFeature.retweetFre
                self.rvp = MongoClient().userFeature.rvp
        
            """

            # feature_dict_data[uid] = [Alkit.load_data_help(self.followCnt, uid, 'follower_cnt'),
            #                           Alkit.load_data_help(self.followCnt, uid, 'followee_cnt'),
            #                           Alkit.load_data_help(self.followCnt, uid, 'ff'),
            #                           Alkit.load_data_help(self.followCnt, uid, 'profile'),
            #                           Alkit.load_data_help(self.rvp, uid, 'discription')]

            # if uid in spammer:
            #     result_dict_data[uid] = 1
            # else:
            #     result_dict_data[uid] = -1

            # 第二期改进代码
            if uid in spammer:
                result_dict_data[uid] = 1
            elif uid in normal:
                result_dict_data[uid] = -1
            elif uid in unknown:
                result_dict_data[uid] = 0  # 这个地方是我自己添加的，对于标签未知的用户，设定其标签为0

        return feature_dict_data, result_dict_data


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s : %(levelname)s  %(message)s',
                        datefmt='%Y-%m-%d %A %H:%M:%S')

    user_class = UserClassify('localhost', 'sdh', 'root', 'root', 'utf8')
    user_class.run(train_per=0.8, spammer_per=0.9, reset_dataset=True, dump=True)
    """
    1%, 3%, 5%, 7%, 9%, 11%, 13%, 15%, 17%, 19%, 21%, 23%
    """
    print("start to evaluation")
    user_class.evalutaion()
