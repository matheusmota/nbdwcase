#!/bin/bash

if [ "$EUID" -ne 0 ]
  then echo "Please run as root"
  exit
fi

if [ -x "$(command -v docker)" ]; then
    echo "docker: found"
    # command
else
    echo "Please install docker: https://docs.docker.com/engine/install/ubuntu/"
    exit
fi

if [ -x "$(command -v docker-compose)" ]; then
    echo "docker-compose: found"
    # command
else
    echo "Please install docker-compose: https://docs.docker.com/compose/install/"
    exit
fi



ver=$(python3 -V 2>&1 | sed 's/.* \([0-9]\).\([0-9]\).*/\1\2/')
if [ "$ver" -lt "30" ]; then
    echo "This script requires python 3"
    exit 1
fi
echo "python3: found"

#########################################
# Datahub
#########################################

# installing datahub connector
sudo pip install -q 'acryl-datahub[metabase]'
sudo pip install -q 'acryl-datahub[nifi]'
sudo pip install -q 'acryl-datahub[tableau]'
sudo pip install -q 'acryl-datahub[superset]'
sudo pip install -q 'acryl-datahub[glue]'
sudo pip install -q 'acryl-datahub[mysql]'
sudo pip install -q 'acryl-datahub[airflow]'


#########################################
# Looker
#########################################


#looker as docker (https://github.com/looker/docker_looker)

# git clone git@github.com:looker/docker_looker.git && 
# cd docker_looker &&
# docker build -t looker:latest .


#########################################
# Airflow 
#########################################
# https://airflow.apache.org/docs/apache-airflow/2.0.1/start/docker.html
# mkdir -p resources/docker/airflow 
# cd resources/docker/airflow  && \
# docker-compose down && \
# docker-compose up -d


repo2docker --no-run --image-name 'nbdwcase/platform' ./

#########################################
# My docker-compose
#########################################


docker-compose up