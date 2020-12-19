import copy
import datetime
import json
import os
from collections import defaultdict

import pymongo


class DBMS(object):
    DB_NAME = 'proj'
    DB_USER = 'user'
    DB_ARTICLE = 'article'
    DB_READ = 'read'
    DB_BE_READ = 'beread'
    DB_RANK = 'rank'

    def __init__(self, host="localhost", port=27017):
        self.mongo = pymongo.MongoClient(host, port)
        self.db = self.mongo[self.DB_NAME]

    def mongo_init(self, init_path: str):
        # collections = ['user', 'article', 'read']
        # for collec in collections:
        #     path = os.path.join(init_path, f'{collec}.dat')
        #     with open(path, 'r') as fin:
        #         dat = []
        #         for line in fin:
        #             dat.append(json.loads(line))
        #     self.db[collec].insert_many(dat)
        self.insert_into_user(os.path.join(init_path, 'user.dat'))
        self.insert_into_article(os.path.join(init_path, 'article.dat'))
        self.insert_into_read(os.path.join(init_path, 'read.dat'))
        self.insert_into_be_read()
        self.insert_into_rank()

    def is_empty(self):
        if DBMS.DB_NAME not in self.mongo.list_database_names():
            return True
        elif "user" not in self.db.list_collection_names():
            return True
        elif self.db["user"].count() == 0:
            return True
        else:
            return False

    def echo(self):
        try:
            self.mongo.list_database_names()
        except Exception:
            return False
        return True

    def perform(self, collection_name: str, pipeline: list):
        return self.db[collection_name].aggregate(pipeline)

    @staticmethod
    def pipeline_top_k(attr: str, k: int, reversed_order=False):
        return [
            # First sort all the docs by name
            {"$sort": {attr: -1 if reversed_order else 1}},
            # Take the first 100 of those
            {"$limit": k}
        ]

    @staticmethod
    def pipeline_join(collection_2: str, attr_1: str, attr_2: str = None, output='__joined__', keep_attrs: list = None):
        if attr_2 is None:
            attr_2 = attr_1

        project_dic = {"_id": 0, output: 1}
        if keep_attrs is not None:
            for attr in keep_attrs:
                project_dic[attr] = 1

        return [
            {"$lookup": {
                "from": collection_2,
                "localField": attr_1,
                "foreignField": attr_2,
                "as": output
            }},
            {"$project": project_dic}
        ]

    def insert_into_read(self, source):

        f = open(source, "r")
        data = f.readlines()
        f.close()

        for i in range(len(data)):
            data[i] = json.loads(data[i])

        # add region field
        for line in data:
            v = line["uid"]
            res = self.db[self.DB_USER].find_one({"uid": v})
            region = res["region"]
            line["region"] = region

        self.db[self.DB_READ].insert_many(data)

    def insert_into_user(self, source):

        f = open(source, "r")
        data = f.readlines()
        f.close()

        for i in range(len(data)):
            data[i] = json.loads(data[i])

        self.db[self.DB_USER].insert_many(data)

    def insert_into_article(self, source):
        f = open(source, "r")
        data = f.readlines()
        f.close()
        backup = []

        for i in range(len(data)):
            data[i] = json.loads(data[i])
            if data[i]["category"] == "science":
                backup.append(data[i])

        self.db[self.DB_ARTICLE].insert_many(data)
        self.db['articleBackup'].insert_many(backup)

    def insert_into_be_read(self):
        head = {
            "id": None,
            "timestamp": None,
            "aid": None,
            "category": "",
            "readNum": 0,
            "readUidList": [],
            "commentNum": 0,
            "commentUidList": [],
            "agreeNum": 0,
            "agreeUidList": [],
            "shareNum": 0,
            "shareUidList": [],
        }

        whole = self.db[self.DB_READ].find()

        target = self.db[self.DB_BE_READ]

        count = 0
        for line in whole:
            aid = line["aid"]
            if target.find({"aid": aid}).count() > 0:
                record = target.find_one({"aid": aid})

                if line["readOrNot"] == "1":
                    record["readNum"] = int(record["readNum"]) + 1
                    record["readUidList"].append(line["uid"])

                if line["agreeOrNot"] == "1":
                    record["agreeNum"] = int(record["agreeNum"]) + 1
                    record["agreeUidList"].append(line["uid"])

                if line["commentOrNot"] == "1":
                    record["commentNum"] = int(record["commentNum"]) + 1
                    record["commentUidList"].append(line["uid"])

                if line["shareOrNot"] == "1":
                    record["shareNum"] = int(record["shareNum"]) + 1
                    record["shareUidList"].append(line["uid"])

                target.update_one(
                    {"_id": record["_id"], "aid": aid}, {"$set": record})
                if record["category"] == "science":
                    self.db['bereadBackup'].update_one(
                        {"aid": aid}, {"$set": record})
            else:
                temp = copy.deepcopy(head)
                temp["id"] = f"b{count}"
                count += 1
                temp["timestamp"] = line["timestamp"]
                temp["aid"] = line["aid"]

                if line["readOrNot"] == "1":
                    temp["readNum"] += 1
                    temp["readUidList"].append(line["uid"])

                if line["agreeOrNot"] == "1":
                    temp["agreeNum"] += 1
                    temp["agreeUidList"].append(line["uid"])

                if line["commentOrNot"] == "1":
                    temp["commentNum"] += 1
                    temp["commentUidList"].append(line["uid"])

                if line["shareOrNot"] == "1":
                    temp["shareNum"] += 1
                    temp["shareUidList"].append(line["uid"])

                temp["category"] = self.db[self.DB_ARTICLE].find_one(
                    {"aid": temp["aid"]})["category"]
                target.insert_one(temp)

                if temp["category"] == "science":
                    self.db['bereadBackup'].insert_one(temp)

    def insert_into_rank(self):
        whole = self.db[self.DB_READ].find()
        latest_day = datetime.datetime.utcfromtimestamp(
            1506400000).strftime("%Y-%m-%d")
        latest_week = datetime.datetime.utcfromtimestamp(
            1506400000).strftime("%Y-%W")
        latest_month = datetime.datetime.utcfromtimestamp(
            1506400000).strftime("%Y-%m")
        print(latest_day)

        day_rank = defaultdict(int)
        week_rank = defaultdict(int)
        month_rank = defaultdict(int)

        whole = self.db[self.DB_READ].find()
        for line in whole:
            if line["readOrNot"] == "1":
                t = line["timestamp"][:-3]
                t = int(t)
                day = datetime.datetime.utcfromtimestamp(
                    t).strftime("%Y-%m-%d")
                week = datetime.datetime.utcfromtimestamp(t).strftime("%Y-%W")
                month = datetime.datetime.utcfromtimestamp(t).strftime("%Y-%m")

                if day == latest_day:
                    day_rank[line["aid"]] += 1
                if week == latest_week:
                    week_rank[line["aid"]] += 1
                if month == latest_month:
                    month_rank[line["aid"]] += 1

        day_rank = sorted(day_rank.items(),
                          key=lambda x: int(x[1]), reverse=True)
        week_rank = sorted(week_rank.items(),
                           key=lambda x: int(x[1]), reverse=True)
        month_rank = sorted(month_rank.items(),
                            key=lambda x: int(x[1]), reverse=True)

        day_rank_c = []
        week_rank_c = []
        month_rank_c = []

        article_co = self.db[self.DB_ARTICLE]

        day_rank_backup = []
        day_rank_cate = []

        week_rank_backup = []
        week_rank_cate = []

        month_rank_backup = []
        month_rank_cate = []

        for k, v in day_rank:
            category = article_co.find_one({"aid": k})["category"]
            day_rank_c.append(category)

            if category == "science":
                day_rank_backup.append(k)
                day_rank_cate.append(category)

        for k, v in week_rank:
            category = article_co.find_one({"aid": k})["category"]
            week_rank_c.append(category)

            if category == "science":
                week_rank_backup.append(k)
                week_rank_cate.append(category)

        for k, v in month_rank:
            category = article_co.find_one({"aid": k})["category"]
            month_rank_c.append(category)

            if category == "science":
                month_rank_backup.append(k)
                month_rank_cate.append(category)

        for i in range(len(day_rank)):
            day_rank[i] = day_rank[i][0]

        for i in range(len(week_rank)):
            week_rank[i] = week_rank[i][0]

        for i in range(len(month_rank)):
            month_rank[i] = month_rank[i][0]

        tar = self.db[self.DB_RANK]
        tar.insert_one(
            {
                "id": "p1",
                "timestamp": line["timestamp"],
                "temporalGranularity": "daily",
                "articleAidList": day_rank,
                "category": day_rank_c,
            }
        )

        tar.insert_one(
            {
                "id": "p2",
                "timestamp": line["timestamp"],
                "temporalGranularity": "weekly",
                "articleAidList": week_rank,
                "category": week_rank_c,
            }
        )

        tar.insert_one(
            {
                "id": "p3",
                "timestamp": line["timestamp"],
                "temporalGranularity": "monthly",
                "articleAidList": month_rank,
                "category": month_rank_c,
            }
        )

        tar = self.db['rankBackup']
        tar.insert_one(
            {
                "id": "p1",
                "timestamp": line["timestamp"],
                "temporalGranularity": "daily",
                "articleAidList": day_rank_backup,
                "category": day_rank_cate,
            }
        )

        tar.insert_one(
            {
                "id": "p2",
                "timestamp": line["timestamp"],
                "temporalGranularity": "weekly",
                "articleAidList": week_rank_backup,
                "category": week_rank_cate,
            }
        )

        tar.insert_one(
            {
                "id": "p3",
                "timestamp": line["timestamp"],
                "temporalGranularity": "monthly",
                "articleAidList": month_rank_backup,
                "category": month_rank_cate,
            }
        )


if __name__ == "__main__":

    mongo = DBMS()
    # mongo_init(mongo, args.init_path)
    joined = mongo.perform("read", DBMS.pipeline_top_k(
        'timestamp', 5, reversed_order=True) + DBMS.pipeline_join("article", "aid", output="top_article", keep_attrs=["timestamp"]))

    def extract_res(res, attr):
        out = res[attr]
        if isinstance(out, list):
            assert len(out) == 1
            out = out[0]
        return out

    top_articles = [extract_res(res, "top_article") for res in joined]
    for i in top_articles:
        print(i)
