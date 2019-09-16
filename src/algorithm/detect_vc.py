# -*- coding: utf-8 -*-
# 实现论文Pay Me and I'll Follow You: Detection of Crowdturfing Following Activities in Microblog Environment
# 中的DetectVC算法
from src.algorithm.alkit import Alkit
from src.algorithm.dvc_hits import HITSMapReduce
import logging

from src.util.SqlHelper import SqlHelper
from src.main.evaluation import Evaluation
from sklearn import metrics


class DetectVC(object):

    def __init__(self, h, d, u, p, c, file_name_appendix=''):
        self.host = h
        self.db = d
        self.user = u
        self.passwd = p
        self.charset = c
        self.sqlhelper = SqlHelper(host=self.host, db=self.db, user=self.user, passwd=self.passwd, charset=self.charset)
        self.file_name_appendix = file_name_appendix

        self.user_train_dict, self.user_train_list, self.user_prior_dict, self.user_prior_list = \
            Alkit.read_prior('../main/prior/user_train' + self.file_name_appendix + '.txt',
                             '../main/prior/user_prior' + self.file_name_appendix + '.txt')
        self.spammer, self.spammer_prior, self.normal, self.normal_prior = Alkit.setSN(self.user_train_dict,
                                                                                       self.user_prior_dict)
        self.seed_worker = []
        for uid in self.user_train_dict.keys():
            if self.user_train_dict[uid]['label'] == '1':
                self.seed_worker.append(uid)
        self.other_worker = []
        for uid in self.user_prior_dict.keys():
            if self.user_prior_dict[uid]['label'] == '1':
                self.other_worker.append(uid)
        self.normal = []
        for uid in self.user_prior_dict.keys():
            if self.user_prior_dict[uid]['label'] == '-1':
                self.normal.append(uid)

        self.all_user = self.seed_worker + self.other_worker + self.normal

        self.follow_edge = []

        for uid in self.all_user:
            for result in self.sqlhelper.select_sql('SELECT uid, followeeUid FROM edge WHERE uid=%s' % uid):
                uid = str(result[0])
                followeeUid = str(result[1])
                if followeeUid not in self.all_user:
                    continue
                self.follow_edge.append((uid, followeeUid))

    def run(self):
        """
        主要调用HITS算法，稍作修改就行
        :return: hub, auth
        """
        logging.info('compute hits')
        hub = {}
        auth = {}
        graph = HITSMapReduce(self.all_user, self.follow_edge, self.seed_worker).hits()
        for user in self.all_user:
            hub[user] = graph[user]['hub'][0]
            auth[user] = graph[user]['authority'][0]

        logging.info('用户结果')
        scores = []
        test_result = []
        for uid in self.user_prior_list:
            test_result.append(int(self.user_prior_dict[uid]['label']))
            scores.append(float(hub[uid]))
        user_res = Evaluation.evaluation_self(scores, test_result)

        # ap
        p, r, thresholds = metrics.precision_recall_curve(test_result, scores)
        ap = metrics.average_precision_score(test_result, scores)
        logging.info('user AP:%s' % str(ap))
        with open('../main/detect_vc/user_ap'+self.file_name_appendix+'.txt', 'w') as my_file:
            my_file.write('p r\n')
            for i in range(len(p)):
                my_file.write('%s %s\n' % (str(p[i]), str(r[i])))

        # roc
        fpr, tpr, thresholds = metrics.roc_curve(test_result, scores)
        logging.info('user AUC:%s' % str(metrics.auc(fpr, tpr)))
        with open('../main/detect_vc/user_roc'+self.file_name_appendix+'.txt', 'w') as my_file:
            my_file.write('fpr tpr\n')
            for i in range(len(fpr)):
                my_file.write('%s %s\n' % (str(fpr[i]), str(tpr[i])))

        # top k precision
        worker_score = {}
        for i in range(len(scores)):
            worker_score[self.user_prior_list[i]] = scores[i]
        worker_score = sorted(worker_score.items(), key=lambda im: float(im[1]), reverse=True)
        with open('../main/detect_vc/res_user_top'+self.file_name_appendix+'.txt', 'w') as my_file:
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

        hub = sorted(hub.items(), key=lambda im: float(im[1]), reverse=True)
        with open('../main/detect_vc/hub'+self.file_name_appendix+'.txt', 'w') as my_file:
            my_file.write('type uid hub worker_per total_per\n')
            worker_count_now = 0
            worker_count_all = len(self.other_worker)
            all_count_now = 0
            all_count_all = len(self.all_user) - len(self.seed_worker)
            for itm in hub:
                uid = str(itm[0])
                u_type = '-'
                if uid in self.seed_worker:
                    continue
                if uid in self.other_worker:
                    u_type = 'o'
                    worker_count_now += 1
                if uid in self.normal:
                    u_type = 'n'
                all_count_now += 1

                hub_score = str(itm[1])
                worker_per = str(float(worker_count_now) / worker_count_all)
                total_per = str(float(all_count_now) / all_count_all)
                my_file.write(u_type + ' ' + uid + ' ' + hub_score + ' ' + worker_per + ' ' + total_per + '\n')

        return hub, auth


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s : %(levelname)s  %(message)s',
                        datefmt='%Y-%m-%d %A %H:%M:%S')
    dvc = DetectVC()
    dvc.run()
