#!/usr/bin/env bash

apt-get update > /dev/null 
apt-get install wget -y > /dev/null 

PUBLIC_IP=$(wget -qO- ipinfo.io/ip) 
echo External IP: $PUBLIC_IP 

echo 'Waiting for services...' 
/tools/wait-for-it.sh -q -t 500 spark:8888    -- echo 'Spark/Jupyter is ready!  http://127.0.0.1:8888      or' http://$PUBLIC_IP:8888      
/tools/wait-for-it.sh -q -t 500 nifi:8080     -- echo 'Apache NiFi   is ready!  http://127.0.0.1:8787/nifi or' http://$PUBLIC_IP:8080/nifi 
/tools/wait-for-it.sh -q -t 500 minio-s3:9000 -- echo 'Minio (S3)    is ready!  http://127.0.0.1:9001      or' http://$PUBLIC_IP:9000      
/tools/wait-for-it.sh -q -t 500 metabase:3000 -- echo 'Metabase      is ready!  http://127.0.0.1:3000      or' http://$PUBLIC_IP:3000      
/tools/wait-for-it.sh -q -t 500 spark:8998    -- echo 'Apache Livy   is ready!  http://127.0.0.1:8998      or' http://$PUBLIC_IP:8998      
# /tools/wait-for-it.sh -q -t 500 presto:8080   -- echo 'Presto        is ready!  http://127.0.0.1:18080     or' http://$PUBLIC_IP:18080     
echo 'All services checked.'

exit 0