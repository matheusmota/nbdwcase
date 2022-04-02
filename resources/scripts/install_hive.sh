#!/usr/bin/env bash

export HIVE_VERSION=2.3.9  && echo $HIVE_VERSION


wget https://dlcdn.apache.org/hive/hive-2.3.9/apache-hive-2.3.9-bin.tar.gz -O apache-hive-2.3.9-bin.tar.gz &&
tar -xf apache-hive-2.3.9-bin.tar.gz && rm apache-hive-2.3.9-bin.tar.gz
mv       apache-hive-${HIVE_VERSION}-bin        hive-${HIVE_VERSION}


echo 'export HIVE_VERSION=${HIVE_VERSION}' >> ~/.bashrc  
echo 'export HIVE_VERSION=${HIVE_VERSION}' >> ~/.profile 
export HIVE_HOME=/home/jovyan/hive-${HIVE_VERSION} 
export PATH=$PATH:$HIVE_HOME/bin
echo 'export HIVE_HOME=${HIVE_HOME}'       >> ~/.bashrc 
echo 'export HIVE_HOME=${HIVE_HOME}'       >> ~/.profile 
echo 'export PATH=\$PATH:\$HIVE_HOME/bin'  >> ~/.bashrc 
echo 'export PATH=\$PATH:\$HIVE_HOME/bin'  >> ~/.profile       

cp ./nbdwcase/resources/configs/hive/${HIVE_VERSION}/beeline-log4j2.properties ${HIVE_HOME}/conf/  &&
cp ./nbdwcase/resources/configs/hive/${HIVE_VERSION}/hive-site.xml             ${HIVE_HOME}/conf/ &&
# cp ./nbdwcase/resources/configs/hive/${HIVE_VERSION}/minio_jars/*             ${HIVE_HOME}/lib/ &&
# cp ./nbdwcase/resources/configs/hive/${HIVE_VERSION}/mysql-connector-java-8.0.17.jar ${HIVE_HOME}/lib &&

sed -i 's~HIVE_HOME~$HIVE_HOME~g' ${HIVE_HOME}/conf/hive-site.xml

# rm ${HIVE_HOME}/lib/log4j-slf4j-impl-*.jar  &&
${HIVE_HOME}/bin/schematool -dbType derby -initSchema 

echo "Finished starting Hive ${HIVE_VERSION}" 

exit