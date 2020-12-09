import argparse
import json
import os

import redis

from hdfs import WebHDFS
from dbms import DBMS

TMP_DIR = "/var/tmp/proxy"


def get_article(fs, aid: str):
    if not os.path.exists(os.path.join(TMP_DIR, f"article{aid}")):
        fs.get_dir(f"articles/article{aid}", TMP_DIR)
        print(
            f"Article {aid} saved to {os.path.join(TMP_DIR, f'article{aid}')}")
    else:
        print(
            f"Article {aid} already exists at {os.path.join(TMP_DIR, f'article{aid}')}")


def retrieve_top_5(cache, mongo, fs, duration: str) -> list:
    # TODO: change to popular
    # TODO: time duration
    query = f"retrieve_top_5_{duration:s}"
    if cache.exists(query):
        print(f"retrieve query {query} from cache, TTL: {cache.ttl(query)}")
        top_articles = json.loads(cache.get(query))
    else:
        joined = mongo.perform("read", DBMS.pipeline_top_k(
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
        cache.set(query, json.dumps(top_articles), ex=24*60*60)

    for i in top_articles:
        get_article(fs, i['aid'])
    return top_articles


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Proxy for DDBS operations.")
    parser.add_argument('--mongo', dest='mongo_host', default='localhost',
                        help='hostname of MongoDB server (default: localhost)')
    parser.add_argument('--hdfs', dest='hdfs_host', default='localhost',
                        help='hostname of HDFS namenode (default: localhost)')
    parser.add_argument('--path', dest='init_path', default='.',
                        help='path of init data (default: cwd)')

    args = parser.parse_args()

    # connect
    print("Connecting to MongoDB server...")
    mongo = DBMS(args.mongo_host)
    if not mongo.echo():
        print("Error: failed to connect to MongoDB server")
        exit(-1)
    print("MongoDB server connected!")

    print("Connecting to HDFS server...")
    fs = WebHDFS(host=args.hdfs_host)
    if not fs.echo():
        print("Error: failed to connect to HDFS server")
        exit(-1)
    print("HDFS server connected!")

    print("Connecting to local Redis server...")
    caches = {}
    caches['article'] = redis.StrictRedis(host='localhost', port=6379, db=0)
    caches['user'] = redis.StrictRedis(host='localhost', port=6379, db=1)
    caches['read'] = redis.StrictRedis(host='localhost', port=6379, db=2)
    caches['be_read'] = redis.StrictRedis(host='localhost', port=6379, db=3)
    caches['popular'] = redis.StrictRedis(host='localhost', port=6379, db=4)
    for cache in caches.values():
        if cache.echo("ping") != b"ping":
            print("Error: failed to connect to Redis server")
            exit(-1)
    print("Redis server connected!")
    # tmp file dir
    os.makedirs(TMP_DIR, exist_ok=True)

    # init
    if fs.is_empty():
        print("Initializing HDFS...")
        fs.setup(os.path.join(args.init_path, "articles"))
        print("HDFS initialized!")

    if mongo.is_empty():
        print("Initializing MongoDB...")
        mongo.mongo_init(args.init_path)
        print("MongoDB initialized!")

    top_articles = retrieve_top_5(caches['popular'], mongo, fs, 'weekly')
    for i in top_articles:
        print(i)
