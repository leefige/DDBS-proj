# THU DDBS Project

`3-sized-db-generation/` and `db-generation/` credit to course staffs.

## TODO

- [x] HDFS in Docker
- [x] MongoDB in Docker
- [ ] Memcached (or sth. equivalent) in Docker
- [x] Import generated JSON files to MongoDB
- [x] Store unstructured data (images, videos) in HDFS
- [ ] Implement the DDBMS middleware
- [ ] Do we need to implement a frontend (query, check articles/images, etc.)?
- [ ] ...

## MongoDB

Located in `dbms`. This image will start MongoDB service, generate data, and import the documents into MongoDB as collections of `db` named `ddbs`.
Then it moves all the unstructured data (files inside `articles`) to HDFS.

### Build Image

```sh
$ docker build -t dbms ./dbms
```

It may be somehow slow when `apt update` from default sources. Later the sources will be replaced by [TUNA](https://mirrors.tuna.tsinghua.edu.cn/) sources.

### Run Container

Interactively:

```sh
$ docker run -it --network hadoop-cluster dbms
```

`--network hadoop-cluster` connects this container to the same network as the Hadoop cluster.

### Check Container ID

```sh
$ docker ps
```


### Maintain Data in Mongo Shell

Running the container interactively, the Mongo Shell will start automatically.

In Mongo Shell, try:

```mongo
> show dbs
> use ddbs
> show collections
> db.user.find({region:"Beijing"})
> db.user.find({region:"Beijing"}, {_id:0, id:1, name:1, gender:1, region:1})
```

### Stop Container

First check the ID of the container. Then stop this container:

```sh
$ docker rm -f <ID>
```

## Hadoop HDFS

Based on [big-data-europe/docker-hadoop](https://github.com/big-data-europe/docker-hadoop). Re-built on
Ubuntu 20.04 for better package management.

### Build

```sh
$ cd ./docker-hadoop
$ make build
```

For the first time, it takes a bit long to pull Docker images.

### Start

```sh
$ cd ./docker-hadoop
$ make run
```

### Hadoop Web Interface

Visit `http://localhost:9870`.

Use `Utilities -> Browsw the file system` to check files in HDFS.

### Stop

If you want to remove mounted volumes (suggested):

```sh
$ cd ./docker-hadoop
$ make stop
```

Else (not suggested):

```sh
$ cd ./docker-hadoop
$ make stop-keep-volumes
```
