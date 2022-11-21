#!/bin/bash

set -ev

ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys
ssh-keyscan -H localhost >> ~/.ssh/known_hosts
chmod 0600 ~/.ssh/known_hosts
eval `ssh-agent`
ssh-add ~/.ssh/id_rsa

cd /opt
tar -zxf hadoop-3.3.2.tar.gz
mv hadoop-3.3.2 hadoop
cd hadoop
pwd

echo "export HADOOP_HOME=/opt/hadoop" >> ~/.bashrc
echo "export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin" >> ~/.bashrc

source ~/.bashrc

tee etc/hadoop/core-site.xml <<EOF
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
EOF

tee etc/hadoop/hdfs-site.xml <<EOF
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.secondary.http.address</name>
    <value>localhost:9100</value>
  </property>
</configuration>
EOF

bin/hdfs namenode -format
sbin/hadoop-daemon.sh start namenode
sbin/hadoop-daemon.sh start datanode
jps
