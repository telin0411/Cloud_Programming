javac -classpath ../hadoop-core-1.2.1.jar:../hbase-0.94.18.jar -d queryFolder/ javaFolder/queryCourseCS.java
jar -cvf queryCS.jar -C queryFolder . 
javac -classpath ../hadoop-core-1.2.1.jar:../hbase-0.94.18.jar -d queryFolder2/ javaFolder/queryCourseGE.java
jar -cvf queryGE.jar -C queryFolder2 . 
hadoop jar queryCS.jar fp.queryCourseCS Final2/Out_stu_IDs '9961244_transcript_10000' Final2/Query_CS_Data ../inputStudent
hadoop jar queryGE.jar fp.queryCourseGE Final2/Out_stu_IDs '9961244_transcript_10000' Final2/Query_GE_Data ../inputStudent

