GEN_FILE="genTable_mongoDB10G.py"

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

# open Mongo Shell
mongo
