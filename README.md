# THU DDBS Project

`3-sized-db-generation/` and `db-generation/` credit to course staffs.

## TODO

- [x] HDFS in Docker
- [x] MongoDB in Docker
- [ ] Memcached (or sth. equivalent) in Docker
- [x] Import generated JSON files to MongoDB
- [ ] Store unstructured data (images, videos) in HDFS
- [ ] Implement the DDBMS middleware
- [ ] Do we need to implement a frontend (query, check articles/images, etc.)?
- [ ] ...

## MongoDB

Located in `dbms`. This image will start MongoDB service, generate data, and import the documents into MongoDB as collections of `db` named `ddbs`.

### Build Image

```sh
$ docker build -t dbms ./dbms
```

It may be somehow slow when `apt update` from default sources. Later the sources will be replaced by [TUNA](https://mirrors.tuna.tsinghua.edu.cn/) sources.

### Run Container

Interactively:

```sh
$ docker run -it dbms
```

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

Based on [big-data-europe/docker-hadoop](https://github.com/big-data-europe/docker-hadoop).

### Start

```sh
$ cd ./docker-hadoop
$ docker-compose up -d
```

For the first time, it takes a bit long to pull Docker images.

### Hadoop Web Interface

Visit `http://localhost:9870`.

### Stop

If you want to remove mounted volumes:

```sh
$ cd ./docker-hadoop
$ docker-compose down --volumes
```

Else:

```sh
$ cd ./docker-hadoop
$ docker-compose down
```
