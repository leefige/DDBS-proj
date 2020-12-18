#! /usr/bin/env python3

import argparse
import json
import os
import shutil

import redis

from hdfs import WebHDFS
from dbms import DBMS


class Proxy(object):
    TMP_DIR = "/var/tmp/proxy"

    def __init__(self, mongo_host="localhost", hdfs_host="localhost"):
        # connect
        print("Connecting to MongoDB server...")
        self.mongo = DBMS(mongo_host)
        if not self.mongo.echo():
            raise RuntimeError("Error: failed to connect to MongoDB server")
        print("MongoDB server connected!")

        print("Connecting to HDFS server...")
        self.fs = WebHDFS(host=hdfs_host)
        if not self.fs.echo():
            raise RuntimeError("Error: failed to connect to HDFS server")
        print("HDFS server connected!")

        print("Connecting to local Redis server...")
        self.caches = {}
        self.caches['article'] = redis.StrictRedis(
            host='localhost', port=6379, db=0)
        self.caches['user'] = redis.StrictRedis(
            host='localhost', port=6379, db=1)
        self.caches['read'] = redis.StrictRedis(
            host='localhost', port=6379, db=2)
        self.caches['be-read'] = redis.StrictRedis(
            host='localhost', port=6379, db=3)
        self.caches['popular-rank'] = redis.StrictRedis(
            host='localhost', port=6379, db=4)
        self.caches['query'] = redis.StrictRedis(
            host='localhost', port=6379, db=5)

        for cache in self.caches.values():
            if cache.echo("ping") != b"ping":
                raise RuntimeError("Error: failed to connect to Redis server")
        print("Redis server connected!")

        # tmp file dir
        os.makedirs(self.TMP_DIR, exist_ok=True)
        print(f"Proxy temp file directory at {self.TMP_DIR}")

    def init(self, path):
        if self.fs.is_empty():
            print("Initializing HDFS...")
            self.fs.setup(os.path.join(path, "articles"))
            print("HDFS initialized!")

        if self.mongo.is_empty():
            print("Initializing MongoDB...")
            self.mongo.mongo_init(path)
            print("MongoDB initialized!")

    def get_article(self, aid: str):
        if not os.path.exists(os.path.join(self.TMP_DIR, f"article{aid}")):
            self.fs.get_dir(f"articles/article{aid}", self.TMP_DIR)
            print(
                f"Article {aid} saved to {os.path.join(self.TMP_DIR, f'article{aid}')}")
        else:
            print(
                f"Article {aid} already exists at {os.path.join(self.TMP_DIR, f'article{aid}')}")

    def clear_cache(self):
        for cache in self.caches.values():
            cache.flushdb()
        if os.path.exists(self.TMP_DIR):
            shutil.rmtree(self.TMP_DIR)
        os.makedirs(self.TMP_DIR, exist_ok=True)

    def remove_article(self, aid: str):
        if os.path.exists(os.path.join(self.TMP_DIR, f"article{aid}")):
            shutil.rmtree(os.path.join(self.TMP_DIR, f"article{aid}"))

    def query_collection(self, collection: str, condition: dict = None):
        assert collection in ["user", "article",
                              "read", "be-read", "popular-rank"]
        query_cache = self.caches['query']
        if condition is not None:
            assert isinstance(condition, dict)
            query = f"query_collection({collection:s}, {json.dumps(condition):s})"
        else:
            query = f"query_collection({collection:s})"

        if query_cache.exists(query):
            print(
                f"retrieve query {query} from cache, TTL: {query_cache.ttl(query)}")
            ids = json.loads(query_cache.get(query))
            res = [json.loads(self.caches[collection].get(i))
                   for i in ids]
        else:
            res = list(self.mongo.db[collection].find(condition))
            ids = [item['id'] for item in res]
            query_cache.set(query, json.dumps(ids), ex=24*60*60)
            for i in res:
                i.pop('_id')
                self.caches[collection].set(
                    i['id'], json.dumps(i), ex=24*60*60)

        if collection == "article":
            for i in ids:
                self.get_article(i[1:])
        return res

    def query_user_read(self, user_condition: dict):
        query_cache = self.caches['query']
        query = f"query_user_read({json.dumps(user_condition):s})"

        if query_cache.exists(query):
            print(
                f"retrieve query {query} from cache, TTL: {query_cache.ttl(query)}")
            users = json.loads(query_cache.get(query))
        else:
            users = list(self.mongo.db['user'].find(user_condition))
            for user in users:
                user.pop('_id')
                read_list = list(
                    self.mongo.db['read'].find({'uid': user['uid']}))
                for l in read_list:
                    l.pop('_id')
                user['read_list'] = read_list
            query_cache.set(query, json.dumps(users), ex=24*60*60)
        return users

    def insert_one(self, collection: str, content: dict):
        assert collection in ["user", "article", "read"]
        assert 'id' in content.keys()

        _id = self.mongo.db[collection].insert_one(content).inserted_id
        self.caches[collection].delete(content['id'])

        if collection == "article":
            self.remove_article(content['id'][1:])
        return _id

    def update_one(self, collection: str, filter_: dict, update: dict):
        assert collection in ["user", "article", "read"]

        find_res = list(self.mongo.db[collection].find(filter_))
        find_ids = [i['_id'] for i in find_res]

        ret = self.mongo.db[collection].update_one(
            {'_id': find_ids[0]}, update)
        _id = ret.upserted_id
        m_id = self.mongo.db[collection].find({"_id": _id})['id']
        self.caches[collection].delete(m_id)

        if collection == "article":
            self.remove_article(m_id[1:])
        return ret.matched_count, ret.modified_count

    def replace_one(self, collection: str, filter_: dict, replacement: dict):
        assert collection in ["user", "article", "read"]

        ret = self.mongo.db[collection].replace_one(filter_, replacement)
        _id = ret.upserted_id
        m_id = self.mongo.db[collection].find({"_id": _id})['id']
        self.caches[collection].delete(m_id)

        if collection == "article":
            self.remove_article(m_id[1:])
        return ret.matched_count, ret.modified_count

    def retrieve_top_5(self, temporalGranularity: str) -> list:
        popular_cache = self.caches['popular-rank']
        article_cache = self.caches['article']
        query = temporalGranularity
        if popular_cache.exists(query):
            print(
                f"retrieve query {query} from cache, TTL: {popular_cache.ttl(query)}")
            top_article_ids = json.loads(popular_cache.get(query))
            top_articles = [json.loads(article_cache.get(f"a{aid}"))
                            for aid in top_article_ids]
        else:
            # TODO: change to popular
            joined = self.mongo.perform("read", DBMS.pipeline_top_k(
                'timestamp', 5, reversed_order=True) + DBMS.pipeline_join(
                "article", "aid", output="top_article", keep_attrs=["timestamp"]))

            def extract_res(res, attr):
                out = res[attr]
                if isinstance(out, list):
                    assert len(out) == 1
                    out = out[0]
                assert isinstance(out, dict)
                # drop mongo _id
                out.pop("_id")
                return out

            top_articles = [extract_res(res, "top_article") for res in joined]
            top_article_ids = [art['aid'] for art in top_articles]
            popular_cache.set(query, json.dumps(top_article_ids), ex=24*60*60)
            for art in top_articles:
                article_cache.set(f"a{art['aid']}",
                                  json.dumps(art), ex=24*60*60)

        for i in top_articles:
            self.get_article(i['aid'])
        return top_articles


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Proxy for DDBS operations.")
    parser.add_argument('--mongo', dest='mongo_host', default='mongo',
                        help='hostname of MongoDB server (default: mongo)')
    parser.add_argument('--hdfs', dest='hdfs_host', default='hd-name',
                        help='hostname of HDFS namenode (default: hd-name)')
    parser.add_argument('--path', dest='init_path', default='/data',
                        help='path of init data (default: /data)')

    args = parser.parse_args()

    # init
    proxy = Proxy(args.mongo_host, args.hdfs_host)
    proxy.init(args.init_path)

    # top_articles = proxy.retrieve_top_5('weekly')
    # for i in top_articles:
    #     print(i)
    print(proxy.query_collection('user'))
    print(proxy.query_collection('article', {'aid': "5"}))
    print(proxy.query_collection('read', {'aid': "8"}))
    print(proxy.update_one('user', {'uid': "8"}, {
          '$inc': {'obtainedCredits': 10}}))
    print(proxy.query_user_read({'region': "Hong Kong"}))
