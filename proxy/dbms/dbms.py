import json
import os

import pymongo


class DBMS(object):
    DB_NAME = 'proj'

    def __init__(self, host="localhost", port=27017):
        self.mongo = pymongo.MongoClient(host, port)
        self.db = self.mongo[DBMS.DB_NAME]

    def mongo_init(self, init_path: str):
        collections = ['user', 'article', 'read']
        for collec in collections:
            path = os.path.join(init_path, f'{collec}.dat')
            with open(path, 'r') as fin:
                dat = []
                for line in fin:
                    dat.append(json.loads(line))
            self.db[collec].insert_many(dat)

    def is_empty(self):
        if DBMS.DB_NAME not in self.mongo.list_database_names():
            return True
        elif "user" not in self.db.list_collection_names():
            return True
        elif self.db["user"].count == 0:
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
