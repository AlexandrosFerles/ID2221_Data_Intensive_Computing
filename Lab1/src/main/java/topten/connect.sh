$HADOOP_HOME/sbin/hadoop-daemon.sh start namenode 
$HADOOP_HOME/sbin/hadoop-daemon.sh start datanode

$HADOOP_HOME/bin/hdfs dfs -mkdir -p topten_input 
$HADOOP_HOME/bin/hdfs dfs -ls topten_input
