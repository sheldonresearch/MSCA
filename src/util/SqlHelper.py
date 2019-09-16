# 处理sql的工具类
import pymysql


class SqlHelper:

    def __init__(self, host='localhost', db='sdh', user='root', passwd='root', charset='utf8'):
        self.conn = pymysql.connect(host=host, db=db, user=user, passwd=passwd, charset=charset)
        self.cursor = self.conn.cursor()

    def close(self):
        self.cursor.close()
        self.conn.close()

    def select_sql(self, my_sql):
        try:
            # print(my_sql)
            self.cursor.execute(my_sql)
            for result in self.cursor.fetchall():
                yield result
        except:
            # print("select_sql Error!")
            pass

    def select_cnt(self, my_sql):
        try:
            self.cursor.execute(my_sql)
            for result in self.cursor.fetchall():
                return result[0]
        except Exception as e:
            print("select_cnt Error! %s" % str(e))
            print(my_sql)

    def select_sql_first(self, my_sql):
        try:
            self.cursor.execute(my_sql)
            res = self.cursor.fetchone()
            if res:
                return res[0]
            else:
                return -1
        except:
            print(my_sql)
            print("select_sql_first Error!")

    def select_sql_one(self, my_sql):
        li = []
        try:
            self.cursor.execute(my_sql)
            for result in self.cursor.fetchall():
                li.append(str(result[0]))
        except:
            pass
            # print("select_sql_one Error!")
        finally:
            return li

    def select_sql_exist(self, my_sql):
        try:
            self.cursor.execute(my_sql)
            for _ in self.cursor.fetchall():
                return True
        except:
            print("select_sql_exist Error!")
        return False

    def select_sql_source(self, my_sql):
        di = {}
        try:
            self.cursor.execute(my_sql)
            for result in self.cursor.fetchall():
                di[str(result[0])] = str(result[1])
        except:
            print("MySQL Error!")
        finally:
            return di

    def select_sql_two(self, my_sql):
        followee_dict = {}
        follower_dict = {}
        try:
            self.cursor.execute(my_sql)
            for result in self.cursor.fetchall():
                uid = str(result[0])
                followeeUid = str(result[1])
                if uid not in followee_dict.keys():
                    followee_dict[uid] = set()
                followee_dict[uid].add(followeeUid)

                if followeeUid not in follower_dict.keys():
                    follower_dict[followeeUid] = set()
                follower_dict[followeeUid].add(uid)
        except Exception as e:
            print("MySQL Error! %s" % str(e))
        finally:
            return followee_dict, follower_dict

    def insert_or_update_sql(self, my_sql):
        try:
            self.cursor.execute(my_sql)
            self.conn.commit()
        except Exception as e:
            print("MySQL Error! %s" % str(e))