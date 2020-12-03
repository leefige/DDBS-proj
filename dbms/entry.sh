#!/bin/bash

# start MongoDB service
echo "Starting MongoDB..."
# systemctl daemon-reload
# systemctl start mongod
# systemctl status mongod
mlaunch init --replicaset --sharded 3 --nodes 3 --config 3 --mongos 1 --dir $DATA_DIR --hostname $CONTAINER_NAME --bind_ip_all
echo "MongoDB started."

# import JSON files to MongoDB
# mongoimport --db ddbs --collection user --file ./user.dat
# mongoimport --db ddbs --collection article --file ./article.dat
# mongoimport --db ddbs --collection read --file ./read.dat

tail -f /dev/null
