javac -classpath ../hadoop-core-1.2.1.jar:../hbase-0.94.18.jar -d rankFolder/ javaFolder/rankCourse.java
jar -cvf rank.jar -C rankFolder . 
hadoop jar rank.jar fp.rankCourse Final2/Out_stu_IDs Final2/Query_CS_Data Final2/Query_GE_Data ../inputStudent '9961244_category1' '9961244_category2' '9961244_cs_info1' '9961244_cs_info2' Final2/rank_Org '9961244_ge_info1' '9961244_ge_info2'