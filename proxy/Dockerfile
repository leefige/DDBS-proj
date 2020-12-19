# base image
FROM leefige/hadoop-base:ubuntu20.04

# install python3 & pip, using THU source
RUN apt update
RUN apt install -y iputils-ping
RUN apt install -y python3 python3-pip
RUN python3 -m pip install -i https://pypi.tuna.tsinghua.edu.cn/simple pip -U
RUN python3 -m pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple

# install redis
RUN apt update
RUN apt install -y redis-server

# install python requirements
WORKDIR /proxy
COPY ./requirements.txt .
RUN python3 -m pip install -r ./requirements.txt -U

RUN apt update && apt install vim less -y

# copy files
COPY ./hadoop/* /etc/hadoop/
COPY ./3-sized-db-generation .
COPY ./hdfs hdfs
COPY ./dbms dbms
COPY ./proxy.py .
COPY ./entry.sh .
RUN chmod +x ./proxy.py

# start
ENTRYPOINT ["bash", "entry.sh"]
