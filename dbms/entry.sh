#!/bin/bash

# start MongoDB service
echo "Starting MongoDB..."
if [ ! -d $DATA_DIR ]; then
    mlaunch init --replicaset --sharded beijing hongkong --nodes 3 \
        --config 3 --mongos 1 --dir $DATA_DIR --hostname $CONTAINER_NAME --bind_ip_all
    # shard setup
    echo "Setting up shard..."
    mongo ./setup.js
    echo "Finished!"
else
    mlaunch start --dir $DATA_DIR
fi
echo "MongoDB started."

# keep running
tail -f /dev/null
