#!/bin/bash

# start MongoDB service
echo "Starting MongoDB..."
# systemctl daemon-reload
# systemctl start mongod
# systemctl status mongod
mlaunch init --replicaset --sharded beijing hongkong --nodes 3 \
    --config 3 --mongos 1 --dir $DATA_DIR --hostname $CONTAINER_NAME --bind_ip_all
echo "MongoDB started."

# shard setup
echo "Setting up shard..."
mongo ./setup.js
echo "Finished!"

tail -f /dev/null
