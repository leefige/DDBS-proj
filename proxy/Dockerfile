# base image
FROM leefige/hadoop-base:ubuntu20.04

# install python3 & pip, using THU source
RUN apt update
RUN apt install -y iputils-ping
RUN apt install -y python3 python3-pip
RUN python3 -m pip install -i https://pypi.tuna.tsinghua.edu.cn/simple pip -U
RUN python3 -m pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple

# install python requirements
WORKDIR /proxy
COPY ./requirements.txt .
RUN python3 -m pip install -r ./requirements.txt -U

# copy files
COPY ./hadoop/* /etc/hadoop/
COPY ./hdfs hdfs
COPY ./3-sized-db-generation .
COPY ./proxy.py .

# start
ENTRYPOINT ["tail", "-f", "/dev/null"]
