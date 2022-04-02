#!/usr/bin/env bash


python3 -c "import socket; print('spark.hadoop.fs.s3a.endpoint=http://{}:9000'.format(socket.gethostbyname_ex('minio-s3')[2][0]))" 

