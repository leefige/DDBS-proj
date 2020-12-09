#!/bin/bash

# start MongoDB service
echo "Starting Redis..."
service redis-server start
service redis-server status
echo "Redis started."

tail -f /dev/null
