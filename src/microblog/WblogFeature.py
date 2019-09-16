# 实现微博的特征提取功能
import logging

import pymongo
from pymongo import MongoClient
from src.util.SqlHelper import SqlHelper
import re
import jieba
import jieba.analyse
from sklearn import feature_extraction
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import CountVectorizer
import os
import snownlp
import numpy
class WblogFeature:

    pattern_html = re.compile(r'<[^>]+>', re.S)
    pattern_tag = re.compile(r'#.+#', re.S)

    def __init__(self, h, d, u, p, c):
        self.host = h
        self.db = d
        self.user = u
        self.passwd = p
        self.charset = c

    def __enter__(self):
        self.sqlhelper = SqlHelper(host=self.host, db=self.db, user=self.user, passwd=self.passwd, charset=self.charset)
        self.mdb = MongoClient().wblogFeature

        self.swblog = self.sqlhelper.select_sql_one('SELECT wblogId FROM swblog')
        self.wblog = self.sqlhelper.select_sql_one('SELECT wblogId FROM final_wblog WHERE spammer="no"')
        self.unknown = self.sqlhelper.select_sql_one('SELECT wblogId FROM wblog')
        final_wblog = self.sqlhelper.select_sql_one('SELECT wblogId FROM final_wblog WHERE spammer="yes"')
        for wblogId in final_wblog:
            if wblogId not in self.swblog:
                self.swblog.append(wblogId)

        # 不知道为什么spammer和normal两个集合有重合的用户
        # 所以这里简单地将这些重合的用户都认为是spammer
        for uid in self.swblog:
            if uid in self.wblog:
                self.wblog.remove(uid)
        # print(len(swblog))

        for uid in self.swblog:
            if uid in self.unknown:
                self.unknown.remove(uid)
        for uid in self.wblog:
            if uid in self.unknown:
                self.unknown.remove(uid)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.sqlhelper.close()


    def setCommentSimilarity(self):
        """
        计算评论的文本相似度
        将某一条微博下面的所有评论作为语料空间，然后计算基于tf-idf的文本余弦相似度
        :return: none
        """
        col = self.mdb.commentSimilarity
        if not col.find_one():
            logging.info('commentSimilarity为空，设置主键为wblogId')
            col.create_index([('wblogId', pymongo.DESCENDING)], unique=True)

        # swblog = self.sqlhelper.select_sql_one('SELECT wblogId FROM final_wblog WHERE spammer="yes"')
        # wblog = self.sqlhelper.select_sql_one('SELECT wblogId FROM final_wblog WHERE spammer="no"')
        # all_wblog = swblog + wblog

        swblog=self.swblog
        wblog=self.wblog
        unknown=self.unknown
        all_wblog = swblog + wblog+unknown


        # 将“转发微博”这四个字加入了停用词表
        stop_words = WblogFeature.get_stop_words(os.path.dirname(os.getcwd()) + '/microblog/stop_words.txt')

        vectorizer = CountVectorizer(stop_words=stop_words)  # 该类会将文本中的词语转换为词频矩阵，矩阵元素a[i][j] 表示j词在i类文本下的词频

        cc = MongoClient().comment.comment

        for wblogId in all_wblog:
            corpus = []
            try:
                for comment in cc.find({'wblogId': str(wblogId)}):
                    text = self.remove_html(comment['json_text']['text'])
                    # 太短的文本很有可能去停用词后没有 有意义的内容，所以直接不计入计算
                    if len(text) <= 4:
                        continue
                    if wblogId in wblog:
                        text = self.remove_tag(text)
                    corpus.append(' '.join(jieba.cut_for_search(text)))
            except Exception as e:
                logging.error('%s. The wblogId is %s' % (e, str(wblogId)))

            cos_sum = 0.0
            cos_cnt = 0
            try:
                # 第一个fit_transform是计算tf-idf，第二个fit_transform是将文本转为词频矩阵
                tfidf = TfidfTransformer().fit_transform(vectorizer.fit_transform(corpus))
                weight = tfidf.toarray()  # 将tf-idf矩阵抽取出来，元素a[i][j]表示j词在i类文本中的tf-idf权重
                # 计算每两条评论间的余弦相似度
                for i in range(len(weight)):
                    for j in range(len(weight)):
                        if i == j:
                            continue
                        cos_sum += WblogFeature.cos(weight[i], weight[j])
                        cos_cnt += 1
                cos_avg = cos_sum / float(cos_cnt)
            except Exception as e:
                logging.error('%s. The wblogId is %s' % (e, str(wblogId)))
                cos_avg = 0.0

            try:
                if wblogId in swblog:
                    col.insert_one({'wblogId': wblogId, 'swblog': 'true', 'comment_similarity': cos_avg})
                elif wblogId in wblog:
                    col.insert_one({'wblogId': wblogId, 'swblog': 'false', 'comment_similarity': cos_avg})
                elif wblogId in unknown:
                    col.insert_one({'wblogId': wblogId, 'swblog': 'unknown', 'comment_similarity': cos_avg})

            except Exception as e:
                logging.error('%s. The wblogId is %s' % (e, str(wblogId)))
        logging.info('setCommentSimilarity finished')

    def setSentimentSimilarity(self):
        """
        计算评论文本的情感相似度
        使用snownlp（背后是朴素贝叶斯方法）来判断评论的情感，从0（消极）~1（积极）分布，然后计算其标准差
        有待改进：分类精度问题，即目前的情感分类的工具的都很笨，对于复杂一点的句式就不行了，也许用自己以前的可能更好
        :return: none
        """
        col = self.mdb.sentimentSimilarity
        if not col.find_one():
            logging.info('sentimentSimilarity为空，设置主键为wblogId')
            col.create_index([('wblogId', pymongo.DESCENDING)], unique=True)

        # swblog = self.sqlhelper.select_sql_one('SELECT wblogId FROM final_wblog WHERE spammer="yes"')
        # wblog = self.sqlhelper.select_sql_one('SELECT wblogId FROM final_wblog WHERE spammer="no"')
        # all_wblog = swblog + wblog

        swblog = self.swblog
        wblog = self.wblog
        unknown = self.unknown
        all_wblog = swblog + wblog + unknown



        # 有一些评论很短或者没有字之类的
        # 对于这些微博，不参与计算情感极性
        # 过滤的方法是分词后判断去除一个词都不剩下的文本
        stop_words = WblogFeature.get_stop_words(os.path.dirname(os.getcwd()) + '/microblog/stop_words.txt')

        cc = MongoClient().comment.comment

        for wblogId in all_wblog:
            corpus = []
            try:
                for comment in cc.find({'wblogId': str(wblogId)}):
                    text = self.remove_html(comment['json_text']['text'])
                    text = self.remove_tag(text)
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

            std = 0.0
            if len(corpus) > 3:
                sentiment_list = []
                for text in corpus:
                    sentiment_list.append(snownlp.SnowNLP(text).sentiments)
                std = numpy.std(numpy.array(sentiment_list), ddof=1)

            try:
                if wblogId in swblog:
                    col.insert_one({'wblogId': wblogId, 'swblog': 'true', 'sentiment_similarity': std})
                elif wblogId in wblog:
                    col.insert_one({'wblogId': wblogId, 'swblog': 'false', 'sentiment_similarity': std})
                elif wblogId in unknown:
                    col.insert_one({'wblogId': wblogId, 'swblog': 'unknown', 'sentiment_similarity': std})

            except Exception as e:
                logging.error('%s. The wblogId is %s' % (e, str(wblogId)))
        logging.info('setSentimentSimilarity finished')

    def setSpamWords(self):
        """
        从众包营销微博下面的评论中抽取关键词，即tf-idf排名前十的词
        这样对于每一条微博，都能生成十维特征，每一维特征的计算方式为

        :return:
        """
        col = self.mdb.spamWords
        if not col.find_one():
            logging.info('spamWords为空，设置主键为wblogId')
            col.create_index([('wblogId', pymongo.DESCENDING)], unique=True)

        # swblog = self.sqlhelper.select_sql_one('SELECT wblogId FROM final_wblog WHERE spammer="yes"')
        # wblog = self.sqlhelper.select_sql_one('SELECT wblogId FROM final_wblog WHERE spammer="no"')
        # all_wblog = swblog + wblog

        swblog = self.swblog
        wblog = self.wblog
        unknown = self.unknown
        all_wblog = swblog + wblog + unknown

        # 有一些评论很短或者没有字之类的
        # 对于这些微博，不参与计算情感极性
        # 过滤的方法是分词后判断去除一个词都不剩下的文本
        stop_words = WblogFeature.get_stop_words(os.path.dirname(os.getcwd()) + '\\microblog\\stop_words.txt')

        cc = MongoClient().comment.comment
        pass

    def setCommentInteractRatio(self):
        """
        计算给定微博下面的评论之间的互动频率 = 与其他人互动的评论的条数 / 总评论条数
        如何确定是一条互动评论：就简单地看有没有reply_id这个字段，还有@
        :return: none
        """
        col = self.mdb.commentInteractRatio
        if not col.find_one():
            logging.info('commentInteractRatio为空，设置主键为wblogId')
            col.create_index([('wblogId', pymongo.DESCENDING)], unique=True)

        # swblog = self.sqlhelper.select_sql_one('SELECT wblogId FROM final_wblog WHERE spammer="yes"')
        # wblog = self.sqlhelper.select_sql_one('SELECT wblogId FROM final_wblog WHERE spammer="no"')
        # all_wblog = swblog + wblog

        swblog = self.swblog
        wblog = self.wblog
        unknown = self.unknown
        all_wblog = swblog + wblog + unknown



        cc = MongoClient().comment.comment

        for wblogId in all_wblog:
            comment_cnt = 0
            interact_cnt = 0
            try:
                for comment in cc.find({'wblogId': str(wblogId)}):
                    if 'reply_id' in comment['json_text'].keys():
                        interact_cnt += 1
                        continue
                    # text = comment['json_text']['text']
                    # if '>@' in text:
                    #     interact_cnt += 1
                    comment_cnt += 1
            except Exception as e:
                logging.error('%s. The wblogId is %s' % (e, str(wblogId)))

            if comment_cnt == 0:
                interact_ratio = 0.0
            else:
                interact_ratio = float(interact_cnt) / float(comment_cnt)

            try:
                if wblogId in swblog:
                    col.insert_one({'wblogId': wblogId, 'swblog': 'true', 'interact_ratio': interact_ratio})
                elif wblogId in wblog:
                    col.insert_one({'wblogId': wblogId, 'swblog': 'false', 'interact_ratio': interact_ratio})
                elif wblogId in unknown:
                    col.insert_one({'wblogId': wblogId, 'swblog': 'unknown', 'interact_ratio': interact_ratio})
            except Exception as e:
                logging.error('%s. The wblogId is %s' % (e, str(wblogId)))

    def setHotCommentRatio(self):
        """
        计算给定微博的评论中的点赞数与评论数的比例
        :return: none
        """
        col = self.mdb.hotCommentRatio
        if not col.find_one():
            logging.info('hotCommentRatio为空，设置主键为wblogId')
            col.create_index([('wblogId', pymongo.DESCENDING)], unique=True)

        # swblog = self.sqlhelper.select_sql_one('SELECT wblogId FROM final_wblog WHERE spammer="yes"')
        # wblog = self.sqlhelper.select_sql_one('SELECT wblogId FROM final_wblog WHERE spammer="no"')
        # all_wblog = swblog + wblog

        swblog = self.swblog
        wblog = self.wblog
        unknown = self.unknown
        all_wblog = swblog + wblog + unknown



        cc = MongoClient().comment.comment

        for wblogId in all_wblog:
            comment_cnt = 0
            hot_cnt = 0
            try:
                for comment in cc.find({'wblogId': str(wblogId)}):
                    if comment['json_text']['like_counts'] == '':
                        comment_cnt += 1
                    else:
                        hot_cnt += int(comment['json_text']['like_counts'])
                        comment_cnt += 1
            except Exception as e:
                logging.error('%s. The wblogId is %s' % (e, str(wblogId)))

            if comment_cnt == 0:
                hot_ratio = 0.0
            else:
                hot_ratio = float(hot_cnt) / float(comment_cnt)

            try:
                if wblogId in swblog:
                    col.insert_one({'wblogId': wblogId, 'swblog': 'true', 'hot_ratio': hot_ratio})
                elif wblogId in wblog:
                    col.insert_one({'wblogId': wblogId, 'swblog': 'false', 'hot_ratio': hot_ratio})
                elif wblogId in unknown:
                    col.insert_one({'wblogId': wblogId, 'swblog': 'unknown', 'hot_ratio': hot_ratio})
            except Exception as e:
                logging.error('%s. The wblogId is %s' % (e, str(wblogId)))


    @staticmethod
    def remove_html(text):
        """
        去除文本中的html
        :return: 去除html后的文本
        """
        return WblogFeature.pattern_html.sub('', text)

    @staticmethod
    def remove_tag(text):
        """
        去除文本中的标签文本
        :return: 去除标签文本后的文本
        """
        return WblogFeature.pattern_tag.sub('', text)

    @staticmethod
    def remove_html_complete(text):
        """
        去除文本中的html，并提取其中的表情符号
        :return: list[去除html后的文本，表情1，表情2...]
        """
        pass

    @staticmethod
    def get_stop_words(file_path):
        """
        读取停用词文件
        :return: 停用词list
        """
        stop_words = []
        with open(file_path, 'r', encoding='utf-8') as my_file:
            for line in my_file:
                stop_words.append(line.split('\n')[0])
        return stop_words

    @staticmethod
    def cos(vector1, vector2):
        """
        计算余弦相似度
        :param vector1:
        :param vector2:
        :return: 余弦相似度
        """
        dot_product = 0.0
        norm_a = 0.0
        norm_b = 0.0
        for a, b in zip(vector1, vector2):
            dot_product += a * b
            norm_a += a ** 2
            norm_b += b ** 2
        if norm_a == 0.0 or norm_b == 0.0:
            return 0.0
        else:
            return dot_product / ((norm_a * norm_b) ** 0.5)



if __name__ == '__main__':
    # test remove_html
    # for text in MongoClient().comment.comment.find({'wblogId': str('4025678581655349')}):
    #     print(text['json_text']['text'])
    #     print(WblogFeature.remove_html(text['json_text']['text']))
    #     print(WblogFeature.remove_tag(WblogFeature.remove_html(text['json_text']['text'])) + '\n')

    # test jieba
    # sentence = '@@'
    # print(len(sentence))
    # sentence = '你好'
    # print(len(sentence))
    # sentence = '转发微博'
    # print(len(sentence))
    # print(' '.join(jieba.cut_for_search(sentence)))
    # print(len(list(jieba.cut_for_search(sentence))))

    # stop_words = []
    # with open('stop_words.txt', 'r', encoding='utf-8') as my_file:
    #     for line in my_file:
    #         stop_words.append(line.split('\n')[0])
    #
    # cos_sum = 0.0
    # cos_cnt = 0
    # corpus = ["@@",  # 第一类文本切词后的结果，词之间以空格隔开
    #           "他 来到 网易 杭研 大厦",  # 第二类文本的切词结果
    #           "小明 硕士 毕业 与 中国 科学院",  # 第三类文本的切词结果
    #           "我 爱 北京 好 清华大学"]  # 第四类文本的切词结果
    # vectorizer = CountVectorizer(stop_words=stop_words)  # 该类会将文本中的词语转换为词频矩阵，矩阵元素a[i][j] 表示j词在i类文本下的词频
    # transformer = TfidfTransformer()  # 该类会统计每个词语的tf-idf权值
    # tfidf = transformer.fit_transform(
    #     vectorizer.fit_transform(corpus))  # 第一个fit_transform是计算tf-idf，第二个fit_transform是将文本转为词频矩阵
    # word = vectorizer.get_feature_names()  # 获取词袋模型中的所有词语
    # weight = tfidf.toarray()  # 将tf-idf矩阵抽取出来，元素a[i][j]表示j词在i类文本中的tf-idf权重
    # for i in range(len(weight)):  # 打印每类文本的tf-idf词语权重，第一个for遍历所有文本，第二个for便利某一类文本下的词语权重
    #     print(u"-------这里输出第", i, u"类文本的词语tf-idf权重------")
    #     for j in range(len(word)):
    #         print(word[j], weight[i][j])
    # for i in range(len(weight)):
    #     for j in range(len(weight)):
    #         if i == j:
    #             continue
    #         cos_sum += WblogFeature.cos(weight[i], weight[j])
    #         cos_cnt += 1
    # cos_avg = cos_sum / float(cos_cnt)
    # print(cos_avg)

    # 测试情感分析
#     import nltk
#     nltk.download('punkt')
#     import textblob
#     text = '''
#     The titular threat of The Blob has always struck me as the ultimate movie
# monster: an insatiably hungry, amoeba-like mass able to penetrate
# virtually any safeguard, capable of--as a doomed doctor chillingly
# describes it--"assimilating flesh on contact.
# Snide comparisons to gelatin be damned, it's a concept with the most
# devastating of potential consequences, not unlike the grey goo scenario
# proposed by technological theorists fearful of
# artificial intelligence run rampant.
#     '''
#     blob = textblob.TextBlob(text)
#     for sentence in blob.sentences:
#         print(sentence.sentiment.polarity)

    # sli = []
    # sli.append(snownlp.SnowNLP('很美的银饰呢，关注转发随机送美衣').sentiments)
    # sli.append(snownlp.SnowNLP('很美的银饰呢').sentiments)
    # sli.append(snownlp.SnowNLP('恐怖').sentiments)
    # print(snownlp.SnowNLP('很美的银饰呢，关注转发随机送美衣').sentiments)
    # print(snownlp.SnowNLP('很美的银饰呢').sentiments)
    # print(snownlp.SnowNLP('恐怖').sentiments)
    # print(numpy.std(numpy.array(sli), ddof=1))
    #
    # print(snownlp.SnowNLP('很美的银饰呢，关注转发随机送美衣').sim('很美的银饰呢'))
    # print(snownlp.SnowNLP('恐怖').sim('很美的银饰呢'))

    # 测试reply_id 和@
    # comment_cnt = 0
    # interact_cnt = 0
    # try:
    #     for comment in MongoClient().comment.comment.find({'wblogId': '4002259522407752'}):
    #         if 'reply_id' in comment['json_text'].keys():
    #             interact_cnt += 1
    #             continue
    #         text = comment['json_text']['text']
    #         if '>@' in text:
    #             print(comment['json_text']['text'])
    # except Exception as e:
    #     logging.error('%s. The wblogId is %s' % (e, '4002259522407752'))
    pass


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s : %(levelname)s  %(message)s',
                        datefmt='%Y-%m-%d %A %H:%M:%S')

    logging.info('开始提取微博特征')
    with WblogFeature('localhost', 'sdh', 'root', 'root', 'utf8') as feature:
        feature.setCommentSimilarity()
        feature.setSentimentSimilarity()
        feature.setCommentInteractRatio()
        feature.setHotCommentRatio()
