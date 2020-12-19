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
        self.caches[DBMS.DB_ARTICLE] = redis.StrictRedis(
            host='localhost', port=6379, db=0)
        self.caches[DBMS.DB_USER] = redis.StrictRedis(
            host='localhost', port=6379, db=1)
        self.caches[DBMS.DB_READ] = redis.StrictRedis(
            host='localhost', port=6379, db=2)
        self.caches[DBMS.DB_BE_READ] = redis.StrictRedis(
            host='localhost', port=6379, db=3)
        self.caches[DBMS.DB_RANK] = redis.StrictRedis(
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

    def clear_cache(self):
        for cache in self.caches.values():
            cache.flushdb()
        if os.path.exists(self.TMP_DIR):
            shutil.rmtree(self.TMP_DIR)
        os.makedirs(self.TMP_DIR, exist_ok=True)

    def get_article(self, aid: str):
        if not os.path.exists(os.path.join(self.TMP_DIR, f"article{aid}")):
            self.fs.get_dir(f"articles/article{aid}", self.TMP_DIR)
            print(
                f"Article {aid} saved to {os.path.join(self.TMP_DIR, f'article{aid}')}")
        else:
            print(
                f"Article {aid} already exists at {os.path.join(self.TMP_DIR, f'article{aid}')}")

    def remove_article(self, aid: str):
        if os.path.exists(os.path.join(self.TMP_DIR, f"article{aid}")):
            shutil.rmtree(os.path.join(self.TMP_DIR, f"article{aid}"))

    def query_collection(self, collection: str, condition: dict = None):
        assert collection in [DBMS.DB_USER, DBMS.DB_ARTICLE,
                              DBMS.DB_READ, DBMS.DB_BE_READ, DBMS.DB_RANK]
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
            res = list(self.mongo.db[collection].find(condition, {'_id': 0}))
            ids = [item['id'] for item in res]
            query_cache.set(query, json.dumps(ids), ex=60)
            for i in res:
                self.caches[collection].set(
                    i['id'], json.dumps(i), ex=24*60*60)

        if collection == DBMS.DB_ARTICLE:
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
            users = list(self.mongo.perform(DBMS.DB_USER, [
                {'$match': user_condition},
                {'$project': {'_id': 0}}
            ]))
            for user in users:
                # save user to cache
                self.caches[DBMS.DB_USER].set(
                    user['id'], json.dumps(user), ex=24*60*60)
                # query read list
                read_list = list(self.mongo.perform(DBMS.DB_READ, [
                    {'$match': {'uid': user['uid']}},
                    {'$project': {'_id': 0, 'aid': 1}}
                ]))
                user['read_list'] = [self.mongo.db[DBMS.DB_ARTICLE].find_one(
                    {'aid': ar['aid']}, {'_id': 0}) for ar in read_list]
                # save article to cache
                for ar in user['read_list']:
                    self.caches[DBMS.DB_ARTICLE].set(
                        ar['id'], json.dumps(ar), ex=24*60*60)
            query_cache.set(query, json.dumps(users), ex=60)

        # get article
        for user in users:
            for ar in user['read_list']:
                self.get_article(ar['aid'])
        return users

    def insert_one(self, collection: str, content: dict):
        assert collection in [DBMS.DB_USER, DBMS.DB_ARTICLE, DBMS.DB_READ]
        assert 'id' in content.keys()

        _id = self.mongo.db[collection].insert_one(content).inserted_id

        # flush
        self.caches['query'].flushdb()
        if self.caches[collection].exists(content['id']):
            self.caches[collection].delete(content['id'])

        if collection == DBMS.DB_ARTICLE:
            self.remove_article(content['id'][1:])
        return _id

    def update_one(self, collection: str, filter_: dict, update: dict):
        assert collection in [DBMS.DB_USER, DBMS.DB_ARTICLE, DBMS.DB_READ]

        find_res = self.mongo.db[collection].find_one(filter_)

        ret = self.mongo.db[collection].update_one(
            {'_id': find_res['_id']}, update)

        # flush
        self.caches['query'].flushdb()
        if self.caches[collection].exists(find_res['id']):
            self.caches[collection].delete(find_res['id'])

        if collection == DBMS.DB_ARTICLE:
            self.remove_article(find_res['id'][1:])
        return ret.matched_count, ret.modified_count

    def replace_one(self, collection: str, filter_: dict, replacement: dict):
        assert collection in [DBMS.DB_USER, DBMS.DB_ARTICLE, DBMS.DB_READ]

        find_res = self.mongo.db[collection].find_one(filter_)

        ret = self.mongo.db[collection].replace_one(
            {'_id': find_res['_id']}, replacement)

        # flush
        self.caches['query'].flushdb()
        if self.caches[collection].exists(find_res['id']):
            self.caches[collection].delete(find_res['id'])

        if collection == DBMS.DB_ARTICLE:
            self.remove_article(find_res['id'][1:])
        return ret.matched_count, ret.modified_count

    def retrieve_top_5(self, temporalGranularity: str) -> list:
        popular_cache = self.caches[DBMS.DB_RANK]
        article_cache = self.caches[DBMS.DB_ARTICLE]
        query = temporalGranularity
        if popular_cache.exists(query):
            print(
                f"retrieve query {query} from cache, TTL: {popular_cache.ttl(query)}")
            top_article_ids = json.loads(popular_cache.get(query))
            top_articles = [json.loads(article_cache.get(f"a{aid}"))
                            for aid in top_article_ids]
        else:
            # TODO: change to popular
            top_article_ids = self.mongo.db[DBMS.DB_RANK].find_one(
                {'temporalGranularity': temporalGranularity}, {'articleAidList': 1})['articleAidList']
            if len(top_article_ids) > 5:
                top_article_ids = top_article_ids[:5]
            popular_cache.set(query, json.dumps(top_article_ids), ex=60*60)

            top_articles = [self.mongo.db[DBMS.DB_ARTICLE].find_one(
                {'aid': aid}, {'_id': 0}) for aid in top_article_ids]
            for art in top_articles:
                article_cache.set(art['id'], json.dumps(art), ex=24*60*60)

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

    top_articles = proxy.retrieve_top_5('weekly')
    for i in top_articles:
        print(i)
    print()

    print(proxy.query_collection(DBMS.DB_ARTICLE, {'aid': "5"}))
    print()
    print(proxy.query_collection(DBMS.DB_BE_READ, {'aid': "5"}))
    print()
    print(proxy.query_collection(DBMS.DB_USER, {'uid': "8"}))
    print()
    print(proxy.update_one(DBMS.DB_USER, {'uid': "8"}, {
          '$set': {'language': 'zh'}}))
    print()
    print(proxy.query_collection(DBMS.DB_USER, {'uid': "8"}))
    print()
    hk_read = proxy.query_user_read({'region': "Hong Kong"})
    for r in hk_read:
        print(r)
