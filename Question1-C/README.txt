README

1. Operating Hadoop running 'ssh localhost', 'start-all.sh', and Hadoop NameNode.
2. Making the own directory to store the result and input the csv file to the directory:/user/haeun/input/country-list.csv  and  /user/haeun/input/city_temperature.csv 
3. Running this comment "hadoop jar Homework2C.jar WordCount /user/haeun/input/city_temperature.csv  /output1" to operate the system.
4. If it doesn't work, then you need to export Homework2C java file to Jar
5. If it works perfectly, then run "hdfs dfs -get /output1/part-r-00000 /home/haeun/eclipse-workspace"


INFROMATION
-I didn't calculate "-99" degress, which is invalid temperature
For this question, the key value is the city names in Spain, and the value is the average temperature.
The Class name is WordCount
