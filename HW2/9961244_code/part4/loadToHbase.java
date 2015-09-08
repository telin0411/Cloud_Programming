import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;


public class loadToHbase {
  public static void main(String[] args) throws IOException {

    /**************************
       Initialize HBase Table
      **************************/


    // Initialize Config
    Configuration config = HBaseConfiguration.create();
    config.set("hbase.master","localhost:60000");
    HBaseAdmin hbase = new HBaseAdmin(config);
    
    // Initialize Config
    Configuration config1 = HBaseConfiguration.create();
    config1.set("hbase.master","localhost:60000");
    HBaseAdmin hbase1 = new HBaseAdmin(config1);    

    // Check is table exists. If is, Delete it first
    if(hbase.tableExists(args[1])){
       //delete table here
       hbase.disableTable(args[1]);
       hbase.deleteTable(args[1]);	 	
    }
    // Check is table exists. If is, Delete it first
    if(hbase1.tableExists(args[3])){
       //delete table here
       hbase1.disableTable(args[3]);
       hbase1.deleteTable(args[3]);	 	
    }    

    // Create a new table
    //HTableDescriptor
    HTableDescriptor table1 = new HTableDescriptor(args[1]);
    //HColumnDescriptor col2 = new HColumnDescriptor("PageRank");
    HColumnDescriptor col1 = new HColumnDescriptor("Page");    
    table1.addFamily(col1);
    //table1.addFamily(col2);    
    hbase.createTable(table1);
    //HColumnDescriptor
    
    // Create a new table
    //HTableDescriptor
    HTableDescriptor table2 = new HTableDescriptor(args[3]);
    //HColumnDescriptor col2 = new HColumnDescriptor("PageRank");
    HColumnDescriptor col12 = new HColumnDescriptor("InvIndex");    
    table2.addFamily(col12);
    //table1.addFamily(col2);    
    hbase1.createTable(table2);
    //HColumnDescriptor    
   
    //HTable
    HTable ITable = new HTable(config, args[1]);
    /**************************
       Load inputfile from HDFS
      **************************/
    
    // Load input from HDFS
    Configuration conf = new Configuration();
    FileSystem hdfs = FileSystem.get(conf);
    Path hdfsDir = new Path(args[0]);
    try {
        FileStatus[] inputFiles = hdfs.listStatus(hdfsDir);
        for (int i=0; i<inputFiles.length; i++) {
            FSDataInputStream fin = hdfs.open(inputFiles[i].getPath());
            BufferedReader in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
            String line;
            StringBuffer links = new StringBuffer(100);

            while((line = in.readLine()) != null){
	      // handle the data you get from HDFS
	      String str[] = line.split("\t");
              String str2[] = str[1].split("]");
              for(i=0; i<str2.length; i++){
 	        Put put_data = new Put(str2[i].getBytes());
	        put_data.add("Page".getBytes(),"PageRank".getBytes(),str[0].getBytes());
	        ITable.put(put_data);               
              }
		          // put them into the table you create in HBase
            }
            in.close();
        }
    } catch (IOException e) {
        e.printStackTrace();
    }
    
    //HTable
    HTable ITable1 = new HTable(config1, args[3]);    
    // Load input from HDFS
    Configuration conf1 = new Configuration();
    FileSystem hdfs1 = FileSystem.get(conf1);
    Path hdfsDir1 = new Path(args[2]);
    try {
        FileStatus[] inputFiles1 = hdfs1.listStatus(hdfsDir1);
        for (int i=0; i<inputFiles1.length; i++) {
            FSDataInputStream fin1 = hdfs1.open(inputFiles1[i].getPath());
            BufferedReader in1 = new BufferedReader(new InputStreamReader(fin1, "UTF-8"));
            String line1;
            StringBuffer links1 = new StringBuffer(100);

            while((line1 = in1.readLine()) != null){
		          // handle the data you get from HDFS
		          String str3[] = line1.split("\t");
              //String str4[] = str3[1].split("]");
              //for(i=0; i<str4.length; i++){
              Put put_data1 = new Put(str3[0].getBytes());
              put_data1.add("InvIndex".getBytes(),"Table".getBytes(),str3[1].getBytes());
              ITable1.put(put_data1);               
              //}
		          // put them into the table you create in HBase
            }
            in1.close();
        }
    } catch (IOException e) {
        e.printStackTrace();
    }    
}
}
