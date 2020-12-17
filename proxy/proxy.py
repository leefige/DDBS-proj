#! /usr/bin/env python3

import argparse
import json
import os

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
        self.caches['be_read'] = redis.StrictRedis(
            host='localhost', port=6379, db=3)
        self.caches['popular'] = redis.StrictRedis(
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

    def retrieve_top_5(self, temporalGranularity: str) -> list:
        popular_cache = self.caches['popular']
        article_cache = self.caches['article']
        query = temporalGranularity
        if popular_cache.exists(query):
            print(
                f"retrieve query {query} from cache, TTL: {popular_cache.ttl(query)}")
            top_article_ids = json.loads(popular_cache.get(query))
            top_articles = [json.loads(article_cache.get(aid))
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
                article_cache.set(art['aid'], json.dumps(art))

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
