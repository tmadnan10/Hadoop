export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-amd64
export PATH=${JAVA_HOME}/bin:${PATH}
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar

bin/hadoop com.sun.tools.javac.Main /home/hadoop3/sumX.java
cd ..
jar cf sumx.jar sumX*.class
cd hadoop-2.7.3/
rm -rf sumX
bin/hadoop jar /home/hadoop3/sumx.jar sumX input sumX

bin/hadoop com.sun.tools.javac.Main /home/hadoop3/sumXSquare.java
cd ..
jar cf sumxsquare.jar sumXSquare*.class
cd hadoop-2.7.3/
rm -rf sumXSquare
bin/hadoop jar /home/hadoop3/sumxsquare.jar sumXSquare input sumXSquare

bin/hadoop com.sun.tools.javac.Main /home/hadoop3/sumXCube.java
cd ..
jar cf sumxcube.jar sumXCube*.class
cd hadoop-2.7.3/
rm -rf sumXCube
bin/hadoop jar /home/hadoop3/sumxcube.jar sumXCube input sumXCube

bin/hadoop com.sun.tools.javac.Main /home/hadoop3/sumXToFour.java
cd ..
jar cf sumxtofour.jar sumXToFour*.class
cd hadoop-2.7.3/
rm -rf sumXToFour
bin/hadoop jar /home/hadoop3/sumxtofour.jar sumXToFour input sumXToFour

bin/hadoop com.sun.tools.javac.Main /home/hadoop3/sumY.java
cd ..
jar cf sumy.jar sumY*.class
cd hadoop-2.7.3/
rm -rf sumY
bin/hadoop jar /home/hadoop3/sumy.jar sumY input sumY

bin/hadoop com.sun.tools.javac.Main /home/hadoop3/sumXY.java
cd ..
jar cf sumxy.jar sumXY*.class
cd hadoop-2.7.3/
rm -rf sumXY
bin/hadoop jar /home/hadoop3/sumxy.jar sumXY input sumXY

bin/hadoop com.sun.tools.javac.Main /home/hadoop3/sumXSquareY.java
cd ..
jar cf sumxsy.jar sumXSquareY*.class
cd hadoop-2.7.3/
rm -rf sumXSquareY
bin/hadoop jar /home/hadoop3/sumxsy.jar sumXSquareY input sumXSquareY

c++ merge.cpp
./a.out
