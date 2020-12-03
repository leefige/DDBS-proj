import json
import os
import argparse

from hdfs import WebHDFS
from pymongo import MongoClient

DB_NAME = 'proj'


def mongo_init(mongo_host, init_path):
    mongo = MongoClient(mongo_host)
    print(mongo.list_database_names())

    collections = ['user', 'article', 'read']
    for collec in collections:
        path = os.path.join(init_path, f'{collec}.dat')
        with open(path, 'r') as fin:
            dat = []
            for line in fin:
                dat.append(json.loads(line))
        mongo[DB_NAME][collec].insert_many(dat)


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Proxy for DDBS operations.")
    parser.add_argument('--mongo', dest='mongo_host', default='localhost',
                        help='hostname of MongoDB server (default: localhost)')
    parser.add_argument('--hdfs', dest='hdfs_host', default='localhost',
                        help='hostname of HDFS namenode (default: localhost)')
    parser.add_argument('--path', dest='init_path', default='.',
                        help='path of init data (default: cwd)')

    args = parser.parse_args()

    mongo_init(args.mongo_host, args.init_path)
