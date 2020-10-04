# base image
FROM ubuntu:20.04
RUN apt update

# change to THU source
RUN apt install -y ca-certificates
RUN mv /etc/apt/sources.list /etc/apt/sources.list.bkp
COPY ./sources.list /etc/apt/sources.list
RUN apt update && apt upgrade

# install python3 & pip, using THU source
RUN apt install -y python3 python3-pip
RUN python3 -m pip install -i https://pypi.tuna.tsinghua.edu.cn/simple pip -U
RUN python3 -m pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple

# install python requirements
WORKDIR /dbms
COPY ./requirements.txt .
RUN python3 -m pip install -r ./requirements.txt -U

# copy files
COPY ./3-sized-db-generation .

# generate data
CMD ["python3", "./genTable_mongoDB10G.py"]
