#!/bin/bash

# start MongoDB service
echo "Starting MongoDB..."
if [ ! -f "$DATA_DIR/.mlaunch_startup" ]; then
    mlaunch init --replicaset --sharded beijing hongkong --nodes 3 \
        --config 3 --mongos 1 --dir $DATA_DIR --hostname $CONTAINER_NAME --bind_ip_all
else
    mlaunch start --dir $DATA_DIR
fi
echo "MongoDB started."

# shard setup
echo "Setting up shard..."
mongo ./setup.js
echo "Finished!"

# keep running
python3 ./watch.py watch
