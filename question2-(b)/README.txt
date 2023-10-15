Read Me

1. Operating Hadoop running 'ssh localhost', 'start-all.sh', and Hadoop NameNode.
2. Making the own directory to store the result and input the word file to the directory:/user/haeun/input/input_hw1.txt 
3. Running this comment "hadoop jar MyWordCount2.jar WordCount /user/haeun/input/input_hw1.txt /output2" to operate the system.
4. If it doesn't work, then you need to export MyWordCount2 java file to Jar
5. If it works perfectly, then run "hdfs dfs -get /output2/part-r-00000 /home/haeun/eclipse-workspace"
