import os
import argparse

from hdfs import WebHDFS
from dbms import DBMS


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

    # init
    if fs.is_empty():
        print("Initializing HDFS...")
        fs.setup(args.init_path)
        print("HDFS initialized!")

    if mongo.is_empty():
        print("Initializing MongoDB...")
        mongo.mongo_init(args.init_path)
        print("MongoDB initialized!")

    # top k
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

    if os.path.exists("mytmp"):
        import shutil
        shutil.rmtree("mytmp")
    os.makedirs("mytmp")

    for i in top_articles:
        fs.get_dir(f"articles/article{i['aid']}", "mytmp")
