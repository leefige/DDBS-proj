#!/bin/bash

# start MongoDB service
echo "Starting Redis..."
service redis-server start
service redis-server status
echo "Redis started."

# start app
echo "Starting app..."
mkdir django
cd django
django-admin startproject mysite

cd mysite
python3 manage.py startapp app
cp -r /proxy/dbms .
cp -r /proxy/hdfs .
cp -r /proxy/proxy .

cd app
rm views.py
cp /proxy/app/* .

cd ../mysite
rm urls.py
cp /proxy/mysite/* .

cd ..
python3 manage.py runserver 0.0.0.0:8000
echo "Finished!"

tail -f /dev/null
