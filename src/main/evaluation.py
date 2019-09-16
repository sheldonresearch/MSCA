# -*- coding: utf-8 -*-
# 评价指标
# 包括对于阈值的探讨。
import logging
from sklearn import metrics


class Evaluation(object):

    def __init__(self):
        pass

    @staticmethod
    def evaluation_self(scores, test_result):
        """
        用户和微博评价的时候的共用代码
        :return:
        """
        precision = []
        recall = []
        f1 = []
        thresholds = []
        predict_result = []

        tmp = []
        for i in range(len(scores)):
            tmp.append((scores[i], test_result[i]))
        tmp = sorted(tmp, key=lambda x: x[0])
        scores = []
        test_result = []
        for itm in tmp:
            scores.append(itm[0])
            test_result.append(itm[1])

        for s in scores:
            threshold = s
            for s in scores:
                if s < threshold:
                    predict_result.append(-1)
                else:
                    predict_result.append(1)
            precision.append(metrics.precision_score(test_result, predict_result))
            recall.append(metrics.recall_score(test_result, predict_result))

            f1.append(metrics.f1_score(test_result, predict_result))
            thresholds.append(threshold)
            predict_result = []
        max_index = f1.index(max(f1))
        logging.info('threshold：%s' % thresholds[max_index])
        logging.info('准确率：%s' % precision[max_index])
        logging.info('召回率：%s' % recall[max_index])
        logging.info('F1：%s' % f1[max_index])

        # 写入文件中，为了matlab画图
        # with open('thresholds.txt', 'w') as my_file:
        #     my_file.write('threshold precision recall f1\n')
        #     for i in range(len(thresholds)):
        #         my_file.write('%s %s %s %s\n' % (str(thresholds[i]),str(precision[i]), str(recall[i]), str(f1[i])))

        return f1[max_index]

