javac -classpath ../../hadoop-core-1.2.1.jar -d classFolder/ javaFolder/*
javac -classpath ../../hadoop-core-1.2.1.jar:../../hbase-0.94.18.jar -d lhFolder/ ./loadToHbase.java
jar -cvf CompSim.jar -C classFolder . 
jar -cvf loadToHbase.jar -C lhFolder .
hadoop fs -rmr Final2/compareResult-10000 
hadoop jar CompSim.jar fp.CompSim Final_input/input-10000 Final2/compareResult-10000 ../inputStudent Final2/Out_stu_IDs
#hadoop jar loadToHbase.jar loadToHbase [input1] [hbase Table1]
