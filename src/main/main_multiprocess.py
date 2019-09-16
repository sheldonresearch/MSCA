# 程序的入口，所有的算法都可以在这个类中调用
# 在运行算法之前，先要确保数据库中有相应的数据，包括mysql以及mongodb

# import os
# import sys
# print(os.getcwd())
# os.chdir('D:/WorkSpace-Python/MSCA\\src/main/')
# # os.chdir(sys.path[0])
# print(os.getcwd())

# sys.path.append('D:/WorkSpace-Python/MSCA\\src/')

import logging
from src.algorithm.S3MCD import S3MCD
from src.algorithm.crowd_target import CrowdTarget
from src.algorithm.detect_vc import DetectVC
from src.algorithm.user_classify import UserClassify
from src.algorithm.wblog_classify import WblogClassify
from src.algorithm.msca import MSCA
from src.user.UserFeature import UserFeature
from src.microblog.WblogFeature import WblogFeature


class Main:
    def __init__(self, train_per=0.8, spammer_per=0.9, spam_per=0.9, reset_dataset=True, dump=True,
                 add_unknown_into_model=False):
        self.train_per = train_per
        self.spammer_per = spammer_per
        self.spam_per = spam_per
        self.reset_dataset = reset_dataset
        self.dump = dump
        self.add_unknown_into_model = add_unknown_into_model

        self.file_name_appendix = '_tp_' + str(train_per) + \
                                  '_spammer_' + str(spammer_per) + \
                                  '_spam_' + str(spam_per) + \
                                  '_addunknown_' + str(add_unknown_into_model)

    def setUserFeature(self):
        """
        用户特征提取
        考虑到今后特征的种类可能有修改，所以使用mongo来存储
        :return: none
        """
        logging.info('开始提取用户特征')
        with UserFeature('localhost', 'sdh', 'root', 'root', 'utf8') as feature:
            feature.setRegisterDay()
            feature.setFollowCnt()
            feature.setRVP()
            feature.setOriThirdFre()
            feature.setRetweetFre()
            feature.updateRetweetFre(0, 0, 0)
            feature.updateOriThirdFre(0, 0, 0, 0)
            feature.setFF()
            pass

    def runUserPreClassify(self):
        """
        特征提取完毕之后，使用不同的算法对用户的先验类别进行判定
        :param train_per: 训练集比例
        :param reset_dataset: 是否要生成新的训练集和测试集（建议生成新的之前先对老的进行备份）
        :return: none
        """

        logging.info('开始对用户进行预分类')
        uc = UserClassify(h='localhost', d='sdh', u='root', p='root', c='utf8',
                          train_per=self.train_per,
                          spammer_per=self.spammer_per,
                          reset_dataset=self.reset_dataset,
                          dump=self.dump,
                          add_unknown_into_model=self.add_unknown_into_model,
                          file_name_appendix=self.file_name_appendix)
        uc.run()
        uc.evalutaion()

    def setWblogFeature(self):
        """
        微博特征提取
        同样适用mongo存储特征
        :return: none
        """
        logging.info('开始提取微博特征')
        with WblogFeature('localhost', 'sdh', 'root', 'root', 'utf8') as feature:
            feature.setCommentSimilarity()
            feature.setSentimentSimilarity()
            feature.setCommentInteractRatio()
            feature.setHotCommentRatio()
            pass

    def runWblogPreClassify(self):
        """
         特征提取完毕之后，使用不同的算法对微博的先验类别进行判定
        :param train_per: 训练集比例
        :param reset_dataset: 是否要生成新的训练集和测试集（建议生成新的之前先对老的进行备份）
        :return: none
        """

        logging.info('开始对微博进行预分类')
        wc = WblogClassify(h='localhost', d='sdh', u='root', p='root', c='utf8',
                           train_per=self.train_per,
                           spam_per=self.spam_per,
                           reset_dataset=self.reset_dataset,
                           dump=self.dump,
                           add_unknown_into_model=self.add_unknown_into_model,
                           file_name_appendix=self.file_name_appendix)
        wc.run()
        # train_per=0.8, spam_per=0.1, reset_dataset=False, dump=True
        wc.evalutaion()

    def runMSCA(self, reset_auxiliary=True, workers=8):
        """
        跑CMSCA算法（CrowdSourcing Microblogs and Spammers Co-detecting Algorithm）
        :return: none

         self.train_per = train_per
        self.spammer_per = spammer_per
        self.spam_per = spam_per
        self.reset_dataset = reset_dataset
        self.dump = dump
        self.add_unknown_into_model = add_unknown_into_model

        self.file_name_appendix = '_tp_' + str(train_per) + \
                                  '_spammer_' + str(spammer_per) + \
                                  '_spam_' + str(spam_per) + \
                                  '_rd_' + str(reset_dataset) + \
                                  '_dump_' + str(dump) + \
                                  '_addunknown_' + str(add_unknown_into_model)


        """
        logging.info('开始跑CMSCA算法')
        msca = MSCA(h='localhost', d='sdh', u='root', p='root', c='utf8',
                    file_name_appendix=self.file_name_appendix)
        if reset_auxiliary:
            """
            下面这段代码我们本来试图进行多进程的， 但是随后发现msca.loadFollowRelationship进程结束后
            进行msca.setLaplacian()时， self.follow_edge并没有更新， 仍然是空的， 我们怀疑这可能是因为
            多进程之间参数不共享造成的，因此下面将多进程代码注释掉，仍然沿用以前的顺序执行
            """

            # import multiprocessing
            # logging.info('多进程载入转发关系和关注关系')
            # p1 = multiprocessing.Process(target=msca.loadFollowRelationship, args=(workers,))
            # p2 = multiprocessing.Process(target=msca.loadRetweetRelationship, args=(workers,))
            # p1.start()
            # p2.start()
            # p1.join()
            # logging.info('关注关系载入完毕, 计算拉普拉斯矩阵L')
            # msca.setLaplacian()
            # p2.join()
            # logging.info('转发关系载入完毕, 计算转发矩阵R')
            # msca.setReteetMatrix()
            # logging.info('计算 用户间的 && 微博间的 联系强度')
            # msca.setRelationIntensity(reset_dataset=True)

            logging.info('计算拉普拉斯矩阵L')
            msca.loadFollowRelationship(workers=workers)
            msca.setLaplacian()
            logging.info('计算转发矩阵R')
            msca.loadRetweetRelationship(workers=workers)
            msca.setReteetMatrix()

            logging.info('计算 用户间的 && 微博间的 联系强度')

            msca.setRelationIntensity(reset_dataset=True, workers=workers)
            # msca.setRelationIntensity(reset_dataset=False)

        msca.run(lenda1=1.0, lenda2=1.0, alpha=0.15, beta=0.75, gamma=0.04, theta=1650, iteration_limit=100,
                 change_limit=0.01)

        logging.info('MSCA算法结果')
        msca.evaluation()

    def runCA(self):
        """
        跑对比算法
        :return: none
        """

        logging.info('开始跑S3MCD算法')
        s3mcd = S3MCD(h='localhost', d='sdh', u='root', p='root', c='utf8',
                      file_name_appendix=self.file_name_appendix)
        s3mcd.setFollowRelationship()  # self.loadFollowRelationship() 已经并行化

        s3mcd.setTeetMatrix()  # self.loadTweetRelationship() 已经并行化
        s3mcd.setWblogRelation()  # self.loadWblogRelation() 已经并行化
        s3mcd.run(alpha=0.1, beta=0.1, gamma=0.5, lenda=0.4, iteration_limit=100, change_limit=0.01)
        s3mcd.evaluation()

        """
        上面的代码还存在一些问题，
        下面的代码应经过测试没有问题
        """
        logging.info('开始跑对比算法')

        logging.info('开始跑CrowdTarget算法')
        ct = CrowdTarget(h='localhost', d='sdh', u='root', p='root', c='utf8',
                         file_name_appendix=self.file_name_appendix)
        # ct.feature_retweet_time()
        # ct.feature_third()
        # ct.feature_ur()
        ct.run()
        ct.evalutaion()

        logging.info('开始跑DetectVC算法')
        dvc = DetectVC(h='localhost', d='sdh', u='root', p='root', c='utf8',
                       file_name_appendix=self.file_name_appendix)
        dvc.run()

    @staticmethod
    def data_process():
        """
        处理输出的结果数据，使其更符合“点均匀分布”的要求
        :return:
        """
        x_axis = []
        y_axis = []
        interval = 0.05
        coordinate = []
        tmp = []
        target = 'detect_vc/'
        with open(target + 'user_ap.txt', 'r') as my_file:
            for line in my_file:
                if 'p' in line:
                    continue
                # x_value = line.split('\n')[0].split(' ')[0]
                # y_value = line.split('\n')[0].split(' ')[1]
                # coordinate.append((float(x_value), float(y_value)))

                x_value = line.split('\n')[0].split(' ')[1]
                y_value = line.split('\n')[0].split(' ')[0]
                tmp.append((float(x_value), float(y_value)))

        for itm in reversed(tmp):
            coordinate.append(itm)

        x_pre = coordinate[0][0]
        y_pre = coordinate[0][1]
        cnt = 1
        for i in range(len(coordinate)):
            key_point = cnt * interval
            x_now = coordinate[i][0]
            y_now = coordinate[i][1]
            if i == 0 or i == len(coordinate) - 1:
                x_axis.append(coordinate[i][0])
                y_axis.append(coordinate[i][1])
                if i == 0:
                    while x_now >= key_point:
                        cnt += 1
                        key_point = cnt * interval
                continue

            if x_now < key_point:
                continue
            else:
                # 计算直线
                while key_point <= x_now:
                    x_axis.append(key_point)
                    y_axis.append(Main.straight_line(x_pre, y_pre, x_now, y_now, key_point))
                    cnt += 1
                    key_point = cnt * interval

                x_pre = x_now
                y_pre = y_now
                if key_point <= x_now:
                    cnt += 1

        with open(target + 'tt.txt', 'w') as my_file:
            my_file.write('r p\n')
            for i in range(len(x_axis)):
                print('%s %s\n' % (str(x_axis[i]), str(y_axis[i])))
                my_file.write('%s %s\n' % (str(x_axis[i]), str(y_axis[i])))

    @staticmethod
    def straight_line(x1, y1, x2, y2, x):
        """
        计算直线，并计算出给定x坐标后的y坐标
        不考虑无斜率情况
        :param x1:
        :param y1:
        :param x2:
        :param y2:
        :param x:
        :return:
        """
        slope = (y1 - y2) / (x1 - x2)
        return slope * (x - x1) + y1


if __name__ == '__main__':
    # logging.basicConfig(filename='results_log.txt',
    #                     level=logging.INFO,
    #                     format='%(asctime)s : %(levelname)s  %(message)s',
    #                     datefmt='%Y-%m-%d %A %H:%M:%S')

    logger = logging.getLogger()
    logger.setLevel('INFO')
    BASIC_FORMAT = '%(asctime)s : %(levelname)s  %(message)s'
    DATE_FORMAT = '%Y-%m-%d %A %H:%M:%S'
    formatter = logging.Formatter(BASIC_FORMAT, DATE_FORMAT)
    chlr = logging.StreamHandler()  # 输出到控制台的handler
    chlr.setFormatter(formatter)
    chlr.setLevel('INFO')  # 也可以不设置，不设置就默认用logger的level
    fhlr = logging.FileHandler('results_part3.log')  # 输出到文件的handler
    fhlr.setFormatter(formatter)
    logger.addHandler(chlr)
    logger.addHandler(fhlr)

    train_per = 0.8
    spammer_per = 0.9  # 0.03,0.07, 0.11, 0.15, 0.19, 0.23已经跑过啦;
    """1 %, 3 %, 5 %, 7 %, 9 %, 11 %, 13 %, 15 %, 17 %, 19 %, 21 %, 23 %"""

    spam_per = 0.9
    """2%, 4%, 6%, 8%, 10%, 12%, 14%, 16%, 18%, 20%, 22%, 24%, 26%"""
    reset_dataset = True
    dump = True
    add_unknown_into_model = False

    for train_per in [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]:  #
        logging.info('parameter setting: train_per= ' + str(train_per) +
                     ' spammer_per= ' + str(spammer_per) +
                     ' spam_per= ' + str(spam_per) +
                     ' reset_dataset= ' + str(reset_dataset) +
                     ' dump= ' + str(dump) +
                     ' add_unknown_into_model= ' + str(add_unknown_into_model))

        main = Main(train_per=train_per,
                    spammer_per=spammer_per,
                    spam_per=spam_per,
                    reset_dataset=reset_dataset,
                    dump=dump,
                    add_unknown_into_model=add_unknown_into_model)

        import datetime

        start = datetime.datetime.now()

        # # 用户特征计算
        # main.setUserFeature()
        # """
        # 上一次运行试图把14774个用户全部计算一下特征， 以防以后的进一步使用
        # 处理过程并不长，但是由于尼泽阳源代码有一些代码段被注释掉， 导致处理后的mongoddb数据字段不足
        # 因此这里仍然沿用他以前的数据，这样以来这里的用户也只有两种类型true和false，没有unknown
        # 上面的代码做过一次后就不用再做了
        # """

        # 用户先验类别计算
        main.runUserPreClassify()

        # # 微博特征计算
        # main.setWblogFeature()
        # """
        # 上一次运行试图把把全部微博计算一下特征， 以防以后的进一步使用
        # 但是1500000的两集运算时间太长， 一个晚上只处理了1000条微博。按照这个速度，10万条都需要100个晚上，约30天，因此放弃
        # 就用原来的4629条好了，并且这里也不向用户那样分成了spammer=true，false和unknown, 对于微博按照原来的处理，只有
        # true和false， unknown的没有涉及到
        # 上面的代码做过一次后就不用再做了
        # """

        # 微博先验类别计算
        main.runWblogPreClassify()
        main.runMSCA()
        # main.runCA()

        # import multiprocessing
        #
        # p1 = multiprocessing.Process(target=main.runMSCA)
        # p2 = multiprocessing.Process(target=main.runCA)
        # p1.start()
        # p2.start()
        # p1.join()
        # p2.join()
        #
        # main.data_process()

        end = datetime.datetime.now()
        print("跑完一轮实验需要的时间为： ", end - start)
