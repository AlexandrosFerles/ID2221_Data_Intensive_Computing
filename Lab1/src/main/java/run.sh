export HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath) 
export HBASE_CLASSPATH=$($HBASE_HOME/bin/hbase classpath) 
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$HBASE_CLASSPATH
rm -r topten_classes
mkdir topten_classes  
rm topten.jar
javac -cp $HADOOP_CLASSPATH -d topten_classes topten/TopTen.java
jar -cvf topten.jar -C topten_classes/ .
$HADOOP_HOME/bin/hadoop jar topten.jar topten.TopTen topten_input topten_output
