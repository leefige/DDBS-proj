# base image
FROM ubuntu:20.04
RUN apt update

# change to THU source
RUN apt install -y ca-certificates
RUN mv /etc/apt/sources.list /etc/apt/sources.list.bkp
COPY ./sources.list /etc/apt/sources.list
RUN apt update && apt upgrade
RUN apt install -y python3 python3-pip

# update pip
WORKDIR /dbms
COPY ./requirements.txt .
RUN pip3 install -i https://pypi.tuna.tsinghua.edu.cn/simple pip -U
RUN pip3 config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip3 install -r ./requirements.txt -U

# copy files
COPY ./3-sized-db-generation .

# generate data
CMD ["python3", "./genTable_mongoDB10G.py"]
