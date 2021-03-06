# base image
FROM ubuntu:20.04
ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Asia/Shanghai

# change to THU source
RUN apt update && apt install -y ca-certificates && apt clean
RUN mv /etc/apt/sources.list /etc/apt/sources.list.bkp
COPY ./sources.list /etc/apt/sources.list
RUN apt update && apt upgrade -y

# install python3 & pip, using THU source
RUN apt update
RUN apt install -y python3 python3-pip --fix-missing
RUN python3 -m pip install -i https://pypi.tuna.tsinghua.edu.cn/simple pip -U
RUN python3 -m pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple

# install MongoDB (tuna source does not support Ubuntu 20.04 LTS yet)
RUN apt install -y gnupg wget systemctl
RUN wget -qO - https://www.mongodb.org/static/pgp/server-4.4.asc | apt-key add -
RUN echo "deb [ arch=amd64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/4.4 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-4.4.list
RUN apt update && apt install -y mongodb-org
EXPOSE 27017

# install python requirements
WORKDIR /dbms
COPY ./requirements.txt .
RUN python3 -m pip install -r ./requirements.txt -U

# copy files
COPY ./setup.js .
COPY ./entry.sh .
COPY ./watch.py .

# start
ENTRYPOINT ["sh", "entry.sh"]
