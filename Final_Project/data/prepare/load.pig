sbc = LOAD 'hdfs://master:9000/user/9961244/Final2/sortByClass/part-00000' AS(key: chararray, value: chararray);
STORE sbc into 'hbase://9961244_sortByClass_10000' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('ID:Avg');


