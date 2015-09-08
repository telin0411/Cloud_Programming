javac -classpath ../../hadoop-core-1.2.1.jar:../../hbase-0.94.18.jar -d classFolder/ ./*.java
jar -cvf sortByClass.jar -C classFolder . 
hadoop jar sortByClass.jar fp.sortByClass Final_input/input-10000 Final2/sortByClass

