#!/bin/bash

GEN_FILE="genTable_mongoDB_test.py"

# start MongoDB service
echo "Starting MongoDB..."
systemctl daemon-reload
systemctl start mongod
systemctl status mongod
echo "MongoDB started."

# gen data
echo "Generating data using '$GEN_FILE'..."
python3 $GEN_FILE
echo "Finished."

# import JSON files to MongoDB
mongoimport --db ddbs --collection user --file ./user.dat
mongoimport --db ddbs --collection article --file ./article.dat
mongoimport --db ddbs --collection read --file ./read.dat

# move unstructured data to HDFS
hdfs dfs -mkdir -p articles
hdfs dfs -moveFromLocal ./articles/* articles

# open Mongo Shell
mongo
