# THU DDBS Project

`3-sized-db-generation/` and `db-generation/` credit to course staffs.

## TODO

- [x] HDFS in Docker
- [x] MongoDB in Docker
- [x] Memcached (or sth. equivalent) in Docker
- [x] Import generated JSON files to MongoDB
- [x] Store unstructured data (images, videos) in HDFS
- [x] Implement the DDBMS middleware
- [x] Do we need to implement a frontend (query, check articles/images, etc.)?
- [x] ...

## MongoDB

Located in `dbms`. The documents imported into MongoDB are in the database named `proj`.

### Build Image

```sh
$ cd dbms
$ make build
```

It may be somehow slow when `apt update` from default sources. Later the sources will be replaced by [TUNA](https://mirrors.tuna.tsinghua.edu.cn/) sources.

### Run Container

```sh
$ cd dbms
$ make run
```

### Stop Container

```sh
$ cd dbms
$ make stop
```

## Hadoop HDFS

Based on [big-data-europe/docker-hadoop](https://github.com/big-data-europe/docker-hadoop). Re-built on
Ubuntu 20.04 for better package management.

### Build

```sh
$ cd hadoop-docker
$ make build
```

For the first time, it takes a bit long to pull Docker images.

### Start

```sh
$ cd hadoop-docker
$ make run
```

### Hadoop Web Interface

Visit `http://localhost:9870`.

Use `Utilities -> Browsw the file system` to check files in HDFS.

### Stop

```sh
$ cd hadoop-docker
$ make stop
```


## Proxy

### Build

```sh
$ cd proxy
$ make build
```

### Start

```sh
$ cd proxy
$ make run
```

### Initialization (only once)

You may want to login to one of the proxies

```sh
$ docker exec -it proxy-1 bash
```

Inside this proxy, you firstly need to generate some data

```sh
$ cd 3-sized-db-generation
$ cp genTable_mongoDB10G.py /data
$ cp -r video /data
$ cd /data
$ python3 genTable_mongoDB10G.py
```

After a while, the data should be generated. Then you need to insert or
upload them to corresponding clusters, use this

```sh
$ cd /proxy
$ python3 setup.py
```

This may take some time. Please wait with patience.

### Proxy Python Interface

Login to a proxy

```sh
$ docker exec -it proxy-0 bash
```

In proxy, start a Python interpreter

```bash
$ python3
```

In Python, try

```py
>>> from proxy import Proxy
>>> p = Proxy("mongo", "hd-name")
>>> p.query_collection('user', {'uid':'99'})
>>> p.query_collection('article', {'language':'en'})
>>> p.query_user_read({'uid':'99'})
>>> p.update_one('article', {'aid':'99'}, {'language':'zh'})
>>> p.retrieve_top_5('monthly')
```

### Proxy Web Interface

By default there are 2 proxies. For proxy-0, use port 8000; for proxy-1, use port 8001.

Some examples:

- `http://localhost:8000/app/query/user/u99/`
- `http://localhost:8000/app/update/article/a99/{'language':'en'}/`
- `http://localhost:8000/app/user_read/u99/`
- `http://localhost:8000/app/top/daily/`
- `http://localhost:8000/app/detail/a9999/video/video_a9931_video.flv/`

### Stop

```sh
$ cd proxy
$ make stop
```

