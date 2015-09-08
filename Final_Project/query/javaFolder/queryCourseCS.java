package fp;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;
import java.lang.Math;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
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
import java.text.NumberFormat;

class ValueComparator implements Comparator<String>{
    Map<String, Integer> base;
    public ValueComparator(Map<String, Integer> base) {
      this.base = base;
    }
    // Note: this comparator imposes orderings that are inconsistent with equals.    
    public int compare(String a, String b){
      if(base.get(a) >= base.get(b)) {
        return -1;
      } 
      else{
        return 1;
      } // returning 0 would merge keys
    }
}

public class queryCourseCS {
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
   
    if(hbase.tableExists(args[1])){
	 	  System.out.println("\nTable "+args[1]+" Exists!!");
    }    
   
    //HTable for courses with respect to the IDs
    HTable idCourseTable = new HTable(config, args[1]);
       
    //HTable for inverted index
    //HTable iiTable = new HTable(config1, args[2]);
        
    /**************************
       Load inputfile from HDFS
      **************************/
    
    // Load input from HDFS
    Configuration conf = new Configuration();
    FileSystem hdfs = FileSystem.get(conf);
    //Path hdfsDir = new Path(args[1]);
    
    System.out.println("\n\n********** The Query Results **********");

    // Array lists for query
    ArrayList<String> courseListCS = new ArrayList<String>();
    ArrayList<String> courseCntCS = new ArrayList<String>();    
    
    // Hash maps    
    HashMap<String, Integer> inCSCourses = new HashMap<String, Integer>();    
    HashMap<String, Integer> courseRankCS = new HashMap<String, Integer>();
    HashMap<Integer, String> courseCSIndex = new HashMap<Integer, String>();
    HashMap<String, Double> courseRateCSAvg = new HashMap<String, Double>();
    HashMap<String, Double> courseHardCSAvg = new HashMap<String, Double>();
    ValueComparator vc =  new ValueComparator(courseRankCS);
    TreeMap<String, Integer> sorted_courseRankCS = new TreeMap<String, Integer>(vc);
    
    int i;
    String querySem = new String(); // the semester to be queried
    // Import the compare result from hdfs
    Path inList = new Path(args[0]);
    FileSystem fsIn = FileSystem.get(new Configuration());
    FSDataInputStream fstreamIn = fsIn.open(inList);
    BufferedReader br1 = new BufferedReader(new InputStreamReader(fstreamIn)); 
    String tmp = new String();
    //IO
    while(br1.ready()){
      tmp = br1.readLine(); 
    }       
    fsIn.close();
    System.out.println(tmp);
    String inputList[] = tmp.split(";");
    querySem = inputList[0];
    String inputID[] = inputList[1].split(",");
    String[] inIDList = new String[inputID.length];
    for(i=0; i<inputID.length; i++){
      //inIDList.add(inputID[i]);
      inIDList[i] = inputID[i];
      System.out.println("Rank "+i+":"+" ID="+inIDList[i]);  
    }	
    System.out.println("Query Semester = "+querySem);   
    
    // Print out and rearrange the GE courses
    BufferedReader brIn = new BufferedReader(new FileReader(args[3]));
    while(brIn.ready()){
      tmp = brIn.readLine(); 
    }
    String inCSStr1[] = tmp.split("\t");
    String inCSStr2[] = inCSStr1[1].split(",");
    for(i=0; i<inCSStr2.length; i++){
      if(inCSStr2[i].substring(0,2).equals("CS")){
        inCSCourses.put(inCSStr2[i].substring(0,6), i);
      }
    }
    
    try {
      String idTmp = new String();
      String courseTmp = new String();
      int csCnt = 0;
      for(i=0; i<inIDList.length; i++){
        courseTmp = "";
        idTmp = inIDList[i];          
        Get get_data = new Get(Bytes.toBytes(idTmp));
        Result res = idCourseTable.get(get_data);  
        byte[] value = res.getValue(Bytes.toBytes("ID"), Bytes.toBytes("id"));
        String valueStr = Bytes.toString(value);     
        String strTmp1[] = valueStr.split(",");
        // for CS
        for(int n=0; n<strTmp1.length; n++){
          if(strTmp1[n].substring(strTmp1[n].length()-1).equals(querySem) && !inCSCourses.containsKey(strTmp1[n].substring(0,6))){
            if(!courseCntCS.contains(strTmp1[n].substring(0,7))){
              if(strTmp1[n].substring(0,2).equals("CS")){
                courseCntCS.add(strTmp1[n].substring(0,7));
                courseRankCS.put(strTmp1[n].substring(0,7), 1);
                courseTmp = courseTmp + strTmp1[n] + ",";
                courseCSIndex.put(csCnt, strTmp1[n].substring(0,7));                
                // Avg of the rating
                double rating = Double.parseDouble(strTmp1[n].substring(8,9));
                courseRateCSAvg.put(strTmp1[n].substring(0,7), rating);
                // Avg of the hardness
                double hardness = Double.parseDouble(strTmp1[n].substring(7,8));
                courseHardCSAvg.put(strTmp1[n].substring(0,7), rating);              
                csCnt++;
              }
            }
            else{
              if(strTmp1[n].substring(0,2).equals("CS")){
                int cntTmp = courseRankCS.get(strTmp1[n].substring(0,7));
                cntTmp++;
                courseRankCS.put(strTmp1[n].substring(0,7), cntTmp);
                courseTmp = courseTmp + strTmp1[n] + ",";
                // Deal with the rating
                double rating1 = Double.parseDouble(strTmp1[n].substring(8,9));
                double rating2 = courseRateCSAvg.get(strTmp1[n].substring(0,7));
                rating1 += rating2;
                courseRateCSAvg.put(strTmp1[n].substring(0,7), rating1);
                // Deal with the hardness
                double hardness1 = Double.parseDouble(strTmp1[n].substring(7,8));
                double hardness2 = courseHardCSAvg.get(strTmp1[n].substring(0,7));
                hardness1 += hardness2;
                courseHardCSAvg.put(strTmp1[n].substring(0,7), hardness1);                
              }
            }
          }          
        }
        courseListCS.add(courseTmp);     
      }
      sorted_courseRankCS.putAll(courseRankCS);
      System.out.println(sorted_courseRankCS);
      String csStr = new String();
      for(i=0; i<csCnt; i++){
        csStr = courseCSIndex.get(i);
        double rateAvg = courseRateCSAvg.get(csStr) / courseRankCS.get(csStr); // avg of rating
        courseRateCSAvg.put(csStr, rateAvg); 
        double hardAvg = courseHardCSAvg.get(csStr) / courseRankCS.get(csStr); // avg of hardness
        courseHardCSAvg.put(csStr, hardAvg); 
      }
      
      // Set the output format
      NumberFormat nf = NumberFormat.getInstance();
      nf.setMinimumFractionDigits(2);
      nf.setMaximumFractionDigits(2);
      nf.setGroupingUsed(false);
      for(Object key:sorted_courseRankCS.keySet()) {
        System.out.println(key+":"+courseRankCS.get(key)+";rate_avg="+nf.format(courseRateCSAvg.get(key))+";hard_avg="+nf.format(courseHardCSAvg.get(key)));
      }  
      
      // Output the results as file on hdfs
      Configuration confOut = new Configuration();
      FileSystem fsOut = FileSystem.get(confOut);
      Path outFile = new Path(args[2]);
      FSDataOutputStream out = fsOut.create(outFile);
      BufferedWriter brOut = new BufferedWriter(new OutputStreamWriter(out)); 
      for(Object key:sorted_courseRankCS.keySet()) {
        brOut.write(key+":"+courseRankCS.get(key)+";rate_avg="+nf.format(courseRateCSAvg.get(key))+";hard_avg="+nf.format(courseHardCSAvg.get(key))+"\n");
        //brOut.write(key+":"+nf.format(courseHardCSAvg.get(key))+"\n");
      }
      brOut.close();      
      System.out.println("File stored successfully on HDFS!");   
                        
    } catch (Exception e) {
        e.printStackTrace();
    }
}
}
