# 提供一些不同算法需要都的方法
import numpy
from sklearn import preprocessing
from sklearn.preprocessing import Imputer
import logging


class Alkit(object):

    def __init__(self):
        pass

    @staticmethod
    def load_data_help(mdb, uid, target):
        """
        load_data方法中读取数据库数据时可能有缺失值的情况，避免写很多if-else，所以实现此方法
        :return: 如果缺失，返回numpy.NaN，否则，返回target字段
        """
        try:
            res = mdb.find_one({'uid': uid})
            if res:
                return res[target]
            else:
                return numpy.NaN
        except Exception as e:
            logging.error('load_data_help报错，uid：%s target：%s' % (uid, target))
            logging.error(e)

    @staticmethod
    def load_data_help_w(mdb, wblogId, target):
        """
        load_data_help的wblog版本
        :return: 如果缺失，返回numpy.NaN，否则，返回target字段
        """
        try:
            res = mdb.find_one({'wblogId': wblogId})
            if res:
                # print('res not none! for wblogID: ',wblogId)
                if target in res:
                    return res[target]
                else:
                    return numpy.NaN
            else:
                return numpy.NaN
        except Exception as e:
            logging.error('load_data_help报错，wblogId：%s target：%s' % (wblogId, target))
            logging.error(e)

    @staticmethod
    def process_data(feature_dict_data, result_dict_data):
        """
        处理训练集和测试集的数据
        :return: numpy类型的训练集和测试集数据
        """
        feature = numpy.array(list(feature_dict_data.values()))
        # print('55 feature: \n', feature)
        # """
        # [[ nan  nan  nan  nan  nan]
        #  [ nan  nan  nan  nan  nan]
        #  [ nan  nan  nan  nan  nan]
        #  ...,
        #  [ nan  nan  nan  nan  nan]
        #  [ nan  nan  nan  nan  nan]
        #  [ nan  nan  nan  nan  nan]]
        # """
        feature = Imputer(missing_values='NaN', strategy='mean', axis=0).fit_transform(feature)
        # print('66 feature: \n', feature)    # []

        feature = preprocessing.scale(feature)
        feature = preprocessing.minmax_scale(feature)
        result = numpy.array(list(result_dict_data.values()))
        return feature, result

    @staticmethod
    def write_prior(train_file, prior_file, train_user_set, train_result, test_user_set, test_result, predict_result,
                    predict_result_proba):
        """
        将结果写入文件，供后面的阶段使用
        :param train_file: 写入训练集的文件
        :param prior_file: 写入测试集的文件
        :param train_user_set: 训练集用户id
        :param train_result: 训练集用户label
        :param test_user_set: 测试集用户id
        :param predict_result_proba: 测试集用户的prior
        :param predict_result: 测试集用户的prior_label
        :return: none
        """
        with open(train_file, 'w') as my_file:
            for i in range(len(train_user_set)):
                my_file.write('%s %s\n' % (train_user_set[i], train_result[i]))

        with open(prior_file, 'w') as my_file:
            for i in range(len(test_user_set)):
                my_file.write(
                    '%s %s %s %s\n' % (test_user_set[i], test_result[i], predict_result[i], predict_result_proba[i]))

    @staticmethod
    def read_dataset(train_file, test_file):
        """
        由于每次跑先验类别的时候，如果训练集测试集划分不同的话，就会有不同的结果
        所以提供这个方法来读取已经划分好的训练集和测试集，保证复现性
        :param train_file:
        :param test_file:
        :return:
        """
        train_set = []
        with open(train_file, 'r') as my_file:
            for line in my_file:
                line = line.split('\n')[0]
                my_id = line.split(' ')[0]
                train_set.append(my_id)
        test_set = []
        with open(test_file, 'r') as my_file:
            for line in my_file:
                line = line.split('\n')[0]
                my_id = line.split(' ')[0]
                test_set.append(my_id)
        return train_set, test_set

    @staticmethod
    def read_prior(file_path_train, file_path_prior):
        """
        读取训练集，以及测试集上得到的先验类别
        :param file_path: 文件位置
        :return:
        """
        train_dict = {}
        train_list = []
        with open(file_path_train, 'r') as train_file:
            for line in train_file:
                line = line.split('\n')[0]
                my_id = line.split(' ')[0]
                label = line.split(' ')[1]
                train_dict[my_id] = {'label': label}
                train_list.append(my_id)
        prior_dict = {}
        prior_list = []
        with open(file_path_prior, 'r') as prior_file:
            for line in prior_file:
                line = line.split('\n')[0]
                my_id = line.split(' ')[0]
                label = line.split(' ')[1]
                prior_label = line.split(' ')[2]
                prior = line.split(' ')[3]
                prior_dict[my_id] = {'label': label, 'prior_label': prior_label, 'prior': prior}
                prior_list.append(my_id)
        return train_dict, train_list, prior_dict, prior_list

    @staticmethod
    def setSN(train, prior):
        """
        设置spammer,spammer_prior等
        :return:
        """
        s = []
        s_prior = []
        n = []
        n_prior = []
        for my_id in train.keys():
            if train[my_id]['label'] == '1':
                s.append(my_id)
                s_prior.append(my_id)
            else:
                n.append(my_id)
                n_prior.append(my_id)
        for my_id in prior.keys():
            if prior[my_id]['label'] == '1':
                s.append(my_id)
            else:
                n.append(my_id)
            if prior[my_id]['prior_label'] == '1':
                s_prior.append(my_id)
            else:
                n_prior.append(my_id)
        return s, s_prior, n, n_prior
