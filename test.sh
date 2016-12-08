export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-amd64
export PATH=${JAVA_HOME}/bin:${PATH}
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar

bin/hadoop com.sun.tools.javac.Main ToolMapReduce.java
jar cf ToolMapReduce.jar ToolMapReduce*.class
rm -rf ToolMapReduce
rm -rf Tool
bin/hadoop jar ToolMapReduce.jar ToolMapReduce input ToolMapReduce Tool
