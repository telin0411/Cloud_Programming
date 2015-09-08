import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;
import java.lang.Math;

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


public class queryEngine {
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
   
    //HTable for page rank
    HTable prTable = new HTable(config, args[1]);
    //HTable for inverted index
    HTable iiTable = new HTable(config1, args[0]);
        
    /**************************
       Load inputfile from HDFS
      **************************/
    
    // Load input from HDFS
    Configuration conf = new Configuration();
    FileSystem hdfs = FileSystem.get(conf);
    //Path hdfsDir = new Path(args[1]);
    
    System.out.println("\n\n********** The Query Engine **********");
    System.out.println("Please enter the keyword you'd like to query:");    
    Scanner sc = new Scanner(System.in);
    String inputData = sc.nextLine();
    System.out.print("You required to search for: ");
    System.out.println(inputData);

    // Array lists for query
    ArrayList<String> page = new ArrayList<String>();
    ArrayList<Integer> tf = new ArrayList<Integer>();
    ArrayList<Double> pageRank = new ArrayList<Double>(); 
    ArrayList<Double> sort = new ArrayList<Double>();
    ArrayList<String> rankedPage = new ArrayList<String>();        
        
    try {
      int i;
      String queryWord = new String();
      queryWord = inputData;
      Get get_data = new Get(queryWord.getBytes());
      Result res = iiTable.get(get_data);  
      byte[] value = res.getValue(Bytes.toBytes("InvIndex"), Bytes.toBytes("Table"));
      String valueStr = Bytes.toString(value);      
      //System.out.println("The temp result of "+queryWord+" is:");
      //System.out.println(value);   
      
      if(value!=null){      
      String str1[] = valueStr.split(":::");
      String str2[] = str1[1].split(";;;");    
      for(i=0; i<str2.length; i++){
        String str3[] = str2[i].split("===");
        page.add(str3[0]);
        int tfTmp = Integer.parseInt(str3[1]);        
        tf.add(tfTmp);  
      }
      //System.out.println(page);
      
      // Store the page rank;
      String queryPage = new String();        
      for(i=0; i<page.size(); i++){
        queryPage = page.get(i);
        Get get_PR = new Get(queryPage.getBytes());
        Result resPR = prTable.get(get_PR);  
        byte[] valuePR = resPR.getValue(Bytes.toBytes("Page"), Bytes.toBytes("PageRank"));
        String prStr = Bytes.toString(valuePR);      
        //System.out.print("The PageRank of "+queryPage+" is: ");
        //System.out.println(prStr);    
        double prVal = Double.parseDouble(prStr);
        pageRank.add(prVal);       
      }
      //System.out.println(pageRank);

      // Sorting
      int judges = 0, j = 0;
      int sNum = sort.size();
      double PR = pageRank.get(0);
      sort.add(PR);          
      rankedPage.add(page.get(0));
      for(i=1; i<pageRank.size(); i++){
        PR = pageRank.get(i);
        for(j=0; j<sNum; j++){          
          if(PR >= sort.get(j)){
            sort.add(j, PR);
            rankedPage.add(j, page.get(i));
            judges = 1;     
            break;       
          }
        }
        if(judges == 1){
          judges = 0;
          sNum = sort.size();
        }
        else{
          sort.add(PR);
          rankedPage.add(page.get(i));
          sNum = sort.size();
        }       
      }      
      //System.out.println(sort);   
      //System.out.println(rankedPage);  
            
      // List the page
      int listNum;
      if(sort.size()>10){
        listNum = 10;
      }
      else{
        listNum = sort.size();
      }
      System.out.println("\n******* Query Result *******");
      if(listNum>0){
        for(i=0; i<listNum; i++){
          System.out.println("Page Ranking "+(i+1)+" : "+rankedPage.get(i));
          System.out.println("PageRank = "+sort.get(i));
          System.out.println("-----------------------------------");
        }
      }
      else{
        System.out.println("Sorry the result of "+queryWord+" is empty!");
      }
      }
      else{
        System.out.println("\n******* Query Result *******");      
        System.out.println("Sorry the result of "+queryWord+" is empty!");  
      }
           
      
    } catch (Exception e) {
        e.printStackTrace();
    }
}
}
