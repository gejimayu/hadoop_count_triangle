../bin/hdfs dfs -rm -r /user/m/tes_output1
../bin/hdfs dfs -rm -r /user/m/tes_output2
../bin/hdfs dfs -rm -r /user/m/tes_output3
hadoop com.sun.tools.javac.Main CountTriangle.java
jar cf wc.jar CountTriangle*.class
hadoop jar wc.jar CountTriangle /user/m/input

../bin/hdfs dfs -rm -r /user/m/output1
../bin/hdfs dfs -rm -r /user/m/output2
../bin/hdfs dfs -rm -r /user/m/output3
hadoop com.sun.tools.javac.Main CountTriangle.java
jar cf wc.jar CountTriangle*.class
hadoop jar wc.jar CountTriangle /user/twitter/twitter_rv.net


../bin/hdfs dfs -put -f input.txt /user/m/input