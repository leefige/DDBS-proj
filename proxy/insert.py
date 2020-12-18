import copy
import pymongo
import json
import datetime
from collections import defaultdict


def insert_into_read(source, host, port, database, user, read):

    f = open(source, "r")
    data = f.readlines()
    f.close()

    for i in range(len(data)):
        data[i] = json.loads(data[i])

    mongo_client = pymongo.MongoClient(
        host=host,
        port=port,
    )
    mongo_db = mongo_client[database]

    for line in data:
        v = line["uid"]
        res = mongo_db[user].find_one({"uid": v})
        region = res["region"]
        line["region"] = region
    mongo_db[read].insert_many(data)


def insert_into_user(source, host, port, database, user):

    f = open(source, "r")
    data = f.readlines()
    f.close()

    for i in range(len(data)):
        data[i] = json.loads(data[i])

    mongo_client = pymongo.MongoClient(
        host=host,
        port=port,
    )
    mongo_db = mongo_client[database]

    mongo_db[user].insert_many(data)


def insert_into_article(source, host, port, database, article, article_backup):
    f = open(source, "r")
    data = f.readlines()
    f.close()
    backup = []

    for i in range(len(data)):
        data[i] = json.loads(data[i])
        if data[i]["category"] == "science":
            backup.append(data[i])

    mongo_client = pymongo.MongoClient(
        host=host,
        port=port,
    )
    mongo_db = mongo_client[database]

    mongo_db[article].insert_many(data)
    mongo_db[article_backup].insert_many(backup)


def insert_into_be_read(host, port, database, read, be_read, bereadBackup, article):
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

    mongo_client = pymongo.MongoClient(
        host=host,
        port=port,
    )
    mongo_db = mongo_client[database]

    whole = mongo_db[read].find()

    target = mongo_db[be_read]

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
                mongo_db[bereadBackup].update_one(
                    {"aid": aid}, {"$set": record})
        else:
            temp = copy.deepcopy(head)
            temp["id"] = line["id"]
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

            temp["category"] = mongo_db[article].find_one(
                {"aid": temp["aid"]})["category"]
            target.insert_one(temp)

            if temp["category"] == "science":
                mongo_db[bereadBackup].insert_one(temp)


def insert_into_rank(host, port, database, read, rank, rankBackup, article):
    mongo_client = pymongo.MongoClient(
        host=host,
        port=port,
    )
    mongo_db = mongo_client[database]
    whole = mongo_db[read].find()
    latest_day = datetime.datetime.utcfromtimestamp(0).strftime("%Y-%m-%d")
    latest_week = datetime.datetime.utcfromtimestamp(0).strftime("%Y-%W")
    latest_month = datetime.datetime.utcfromtimestamp(0).strftime("%Y-%m")
    for line in whole:
        t = line["timestamp"][:-3]
        t = int(t)
        day = datetime.datetime.utcfromtimestamp(t).strftime("%Y-%m-%d")
        if day > latest_day:
            latest_day = day
        week = datetime.datetime.utcfromtimestamp(t).strftime("%Y-%W")
        if week > latest_week:
            latest_week = week
        month = datetime.datetime.utcfromtimestamp(t).strftime("%Y-%m")
        if month > latest_month:
            latest_month = month

    day_rank = defaultdict(int)
    week_rank = defaultdict(int)
    month_rank = defaultdict(int)

    whole = mongo_db[read].find()
    for line in whole:
        if line["readOrNot"] == "1":
            t = line["timestamp"][:-3]
            t = int(t)
            day = datetime.datetime.utcfromtimestamp(t).strftime("%Y-%m-%d")
            week = datetime.datetime.utcfromtimestamp(t).strftime("%Y-%W")
            month = datetime.datetime.utcfromtimestamp(t).strftime("%Y-%m")

            if day == latest_day:
                day_rank[line["aid"]] += 1
            if week == latest_week:
                week_rank[line["aid"]] += 1
            if month == latest_month:
                month_rank[line["aid"]] += 1

    day_rank = sorted(day_rank.items(), key=lambda x: int(x[1]), reverse=True)
    week_rank = sorted(week_rank.items(),
                       key=lambda x: int(x[1]), reverse=True)
    month_rank = sorted(month_rank.items(),
                        key=lambda x: int(x[1]), reverse=True)

    day_rank_c = []
    week_rank_c = []
    month_rank_c = []

    article_co = mongo_db[article]

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

    tar = mongo_db[rank]
    tar.insert_one(
        {
            "id": 1,
            "timestamp": line["timestamp"],
            "temporalGranularity": "daily",
            "articleAidList": day_rank,
            "category": day_rank_c,
        }
    )

    tar.insert_one(
        {
            "id": 1,
            "timestamp": line["timestamp"],
            "temporalGranularity": "weekly",
            "articleAidList": week_rank,
            "category": week_rank_c,
        }
    )

    tar.insert_one(
        {
            "id": 1,
            "timestamp": line["timestamp"],
            "temporalGranularity": "monthly",
            "articleAidList": month_rank,
            "category": month_rank_c,
        }
    )

    tar = mongo_db[rankBackup]
    tar.insert_one(
        {
            "id": 1,
            "timestamp": line["timestamp"],
            "temporalGranularity": "daily",
            "articleAidList": day_rank_backup,
            "category": day_rank_cate,
        }
    )

    tar.insert_one(
        {
            "id": 1,
            "timestamp": line["timestamp"],
            "temporalGranularity": "weekly",
            "articleAidList": week_rank_backup,
            "category": week_rank_cate,
        }
    )

    tar.insert_one(
        {
            "id": 1,
            "timestamp": line["timestamp"],
            "temporalGranularity": "monthly",
            "articleAidList": month_rank_backup,
            "category": month_rank_cate,
        }
    )


insert_into_user("/data/user.dat", "localhost", 27017, "proj", "user")
insert_into_article("/data/article.dat", "localhost",
                    27017, "proj", "article", "articleBackup")
insert_into_read("/data/read.dat", "localhost",
                 27017, "proj", "user", "read")
insert_into_be_read("localhost", 27017, "proj", "read",
                    "beread", "bereadBackup", "article")
insert_into_rank("localhost", 27017, "proj", "read",
                 "rank", "rankBackup", "article")

# mongo_client = pymongo.MongoClient(
#     host="localhost",
#     port=27017,
# )
# mongo_db = mongo_client["proj"]
# print(mongo_db.beread.getShardDistribution())
