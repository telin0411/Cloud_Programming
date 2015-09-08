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

public class queryCourseGE {
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
    ArrayList<String> courseListGE = new ArrayList<String>();
    ArrayList<String> courseCntGE = new ArrayList<String>();  
    ArrayList<String> courseCntGE2 = new ArrayList<String>();
    ArrayList<String> inputGE = new ArrayList<String>();
    ArrayList<String> inputGELack = new ArrayList<String>();  
    
    // Hash maps    
    HashMap<String, Integer> inGECourses = new HashMap<String, Integer>();
    HashMap<String, Integer> courseRankGE = new HashMap<String, Integer>();    
    HashMap<Integer, String> courseGEIndex = new HashMap<Integer, String>();
    HashMap<String, Double> courseRateGEAvg = new HashMap<String, Double>();
    HashMap<String, Double> courseHardGEAvg = new HashMap<String, Double>();
    HashMap<String, Integer> courseRankGE2 = new HashMap<String, Integer>();
    HashMap<Integer, String> courseGEIndex2 = new HashMap<Integer, String>();
    HashMap<String, Double> courseRateGEAvg2 = new HashMap<String, Double>();
    HashMap<String, Double> courseHardGEAvg2 = new HashMap<String, Double>();
    ValueComparator vc =  new ValueComparator(courseRankGE);
    TreeMap<String, Integer> sorted_courseRankGE = new TreeMap<String, Integer>(vc);
    ValueComparator vc2 =  new ValueComparator(courseRankGE2);
    TreeMap<String, Integer> sorted_courseRankGE2 = new TreeMap<String, Integer>(vc2);
      
    int i;
    String querySem = new String(); // the semester to be queried
    // Import the compare result from hdfs
    Path inList = new Path(args[0]);
    FileSystem fsIn = FileSystem.get(new Configuration());
    FSDataInputStream fstreamIn = fsIn.open(inList);
    BufferedReader br1 = new BufferedReader(new InputStreamReader(fstreamIn)); 
    String tmp = new String();
    //Rearrange the output format
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
      inIDList[i] = inputID[i];    
    }	    
    
    // Print out and rearrange the GE courses
    BufferedReader brIn = new BufferedReader(new FileReader(args[3]));
    while(brIn.ready()){
      tmp = brIn.readLine(); 
    }
    String inGEStr1[] = tmp.split("\t");
    String inGEStr2[] = inGEStr1[1].split(",");
    for(i=0; i<inGEStr2.length; i++){
      if(inGEStr2[i].substring(0,2).equals("GE")){
        String trait = inGEStr2[i].substring(2,3);
        inGECourses.put(inGEStr2[i].substring(0,6), i);
        if(!inputGE.contains(trait)){
          inputGE.add(trait);
        }
      }
    }    
    System.out.println("GE done = "+inputGE);
    
    // Bring up the lacking GE courses
    String orientStr = new String();
    if(inputGE.size()<5){
      for(int orient=1; orient<8; orient++){
        orientStr = orient+"";
        if(!inputGE.contains(orientStr)){
          inputGELack.add(orientStr);
        }
      }
    }
    System.out.println("Lacking orientations of GE = "+inputGELack);

    System.out.println("Query Semester = "+querySem);   
    try {
      String idTmp = new String();
      String courseTmp = new String();
      int geCnt = 0, geCnt2 = 0;
      for(i=0; i<inIDList.length; i++){
        courseTmp = "";
        idTmp = inIDList[i];   
        Get get_data = new Get(Bytes.toBytes(idTmp));
        Result res = idCourseTable.get(get_data);  
        byte[] value = res.getValue(Bytes.toBytes("ID"), Bytes.toBytes("id"));
        String valueStr = Bytes.toString(value);     
        String strTmp1[] = valueStr.split(",");
        // for GE
        for(int n=0; n<strTmp1.length; n++){
          if(!inGECourses.containsKey(strTmp1[n].substring(0,6))){
            if(!courseCntGE.contains(strTmp1[n].substring(0,7))){
              if(strTmp1[n].substring(0,2).equals("GE") && inputGELack.contains(strTmp1[n].substring(2,3))){
                courseCntGE.add(strTmp1[n].substring(0,7));
                courseRankGE.put(strTmp1[n].substring(0,7), 1);
                courseTmp = courseTmp + strTmp1[n] + ",";
                courseGEIndex.put(geCnt, strTmp1[n].substring(0,7));                
                // Avg of the rating
                double rating = Double.parseDouble(strTmp1[n].substring(8,9));
                courseRateGEAvg.put(strTmp1[n].substring(0,7), rating);
                // Avg of the hardness
                double hardness = Double.parseDouble(strTmp1[n].substring(7,8));
                courseHardGEAvg.put(strTmp1[n].substring(0,7), rating);              
                geCnt++;
              }
              else if(strTmp1[n].substring(0,2).equals("GE") && !inputGELack.contains(strTmp1[n].substring(2,3))){
                courseCntGE2.add(strTmp1[n].substring(0,7));
                courseRankGE2.put(strTmp1[n].substring(0,7), 1);
                courseTmp = courseTmp + strTmp1[n] + ",";
                courseGEIndex2.put(geCnt2, strTmp1[n].substring(0,7));                
                // Avg of the rating
                double rating = Double.parseDouble(strTmp1[n].substring(8,9));
                courseRateGEAvg2.put(strTmp1[n].substring(0,7), rating);
                // Avg of the hardness
                double hardness = Double.parseDouble(strTmp1[n].substring(7,8));
                courseHardGEAvg2.put(strTmp1[n].substring(0,7), rating);              
                geCnt2++;
              }              
            }
            else{
              if(strTmp1[n].substring(0,2).equals("GE") && inputGELack.contains(strTmp1[n].substring(2,3))){
                int cntTmp = courseRankGE.get(strTmp1[n].substring(0,7));
                cntTmp++;
                courseRankGE.put(strTmp1[n].substring(0,7), cntTmp);
                courseTmp = courseTmp + strTmp1[n] + ",";
                // Deal with the rating
                double rating1 = Double.parseDouble(strTmp1[n].substring(8,9));
                double rating2 = courseRateGEAvg.get(strTmp1[n].substring(0,7));
                rating1 += rating2;
                courseRateGEAvg.put(strTmp1[n].substring(0,7), rating1);
                // Deal with the hardness
                double hardness1 = Double.parseDouble(strTmp1[n].substring(7,8));
                double hardness2 = courseHardGEAvg.get(strTmp1[n].substring(0,7));
                hardness1 += hardness2;
                courseHardGEAvg.put(strTmp1[n].substring(0,7), hardness1);                
              }
              else if(strTmp1[n].substring(0,2).equals("GE") && !inputGELack.contains(strTmp1[n].substring(2,3))){
                int cntTmp = courseRankGE2.get(strTmp1[n].substring(0,7));
                cntTmp++;
                courseRankGE2.put(strTmp1[n].substring(0,7), cntTmp);
                courseTmp = courseTmp + strTmp1[n] + ",";
                // Deal with the rating
                double rating1 = Double.parseDouble(strTmp1[n].substring(8,9));
                double rating2 = courseRateGEAvg2.get(strTmp1[n].substring(0,7));
                rating1 += rating2;
                courseRateGEAvg2.put(strTmp1[n].substring(0,7), rating1);
                // Deal with the hardness
                double hardness1 = Double.parseDouble(strTmp1[n].substring(7,8));
                double hardness2 = courseHardGEAvg2.get(strTmp1[n].substring(0,7));
                hardness1 += hardness2;
                courseHardGEAvg2.put(strTmp1[n].substring(0,7), hardness1);                
              }              
            }
          }          
        }
        courseListGE.add(courseTmp);     
      }
      sorted_courseRankGE.putAll(courseRankGE);
      sorted_courseRankGE2.putAll(courseRankGE2);
      String geStr = new String();
      for(i=0; i<geCnt; i++){
        geStr = courseGEIndex.get(i);
        double rateAvg = courseRateGEAvg.get(geStr) / courseRankGE.get(geStr); // avg of rating
        courseRateGEAvg.put(geStr, rateAvg); 
        double hardAvg = courseHardGEAvg.get(geStr) / courseRankGE.get(geStr); // avg of hardness
        courseHardGEAvg.put(geStr, hardAvg);
      }
      for(i=0; i<geCnt2; i++){
        geStr = courseGEIndex2.get(i);
        double rateAvg2 = courseRateGEAvg2.get(geStr) / courseRankGE2.get(geStr); // avg of rating
        courseRateGEAvg2.put(geStr, rateAvg2); 
        double hardAvg2 = courseHardGEAvg2.get(geStr) / courseRankGE2.get(geStr); // avg of hardness
        courseHardGEAvg2.put(geStr, hardAvg2); 
      }      
      
      // Set the output format
      int semester = Integer.parseInt(querySem) % 2;
      NumberFormat nf = NumberFormat.getInstance();
      nf.setMinimumFractionDigits(2);
      nf.setMaximumFractionDigits(2);
      nf.setGroupingUsed(false);
      for(Object key:sorted_courseRankGE.keySet()) {
        System.out.println(key+":"+courseRankGE.get(key)+";rate_avg="+nf.format(courseRateGEAvg.get(key))+";hard_avg="+"0.00");
      }  
      for(Object key:sorted_courseRankGE2.keySet()) {
        System.out.println(key+":"+courseRankGE2.get(key)+";rate_avg="+nf.format(courseRateGEAvg2.get(key))+";hard_avg="+"0.00");
      }        
      
      // Output the results as file on hdfs
      Configuration confOut = new Configuration();
      FileSystem fsOut = FileSystem.get(confOut);
      Path outFile = new Path(args[2]);
      FSDataOutputStream out = fsOut.create(outFile);
      BufferedWriter brOut = new BufferedWriter(new OutputStreamWriter(out)); 
      for(Object key:sorted_courseRankGE.keySet()) {
        int outSem = Integer.parseInt(key.toString().substring(3,4));
        if(outSem%2==semester){
          brOut.write(key+":"+courseRankGE.get(key)+";rate_avg="+nf.format(courseRateGEAvg.get(key))+";hard_avg="+nf.format(courseHardGEAvg.get(key))+"\n");
        }
        //brOut.write(key+":"+nf.format(courseHardGEAvg.get(key))+"\n");
        //brOut.write(key+":"+"0.00"+"\n");
      }
      for(Object key:sorted_courseRankGE2.keySet()) {
        int outSem = Integer.parseInt(key.toString().substring(3,4));
        if(outSem%2==semester){      
          brOut.write(key+":"+courseRankGE2.get(key)+";rate_avg="+nf.format(courseRateGEAvg2.get(key))+";hard_avg="+nf.format(courseHardGEAvg2.get(key))+"\n");
        }
        //brOut.write(key+":"+nf.format(courseHardGEAvg.get(key))+"\n");
        //brOut.write(key+":"+"0.00"+"\n");
      }     
      String geDone = new String();
      geDone = "";
      for(i=0; i<inputGE.size();i++){
        geDone = geDone + inputGE.get(i) + ",";
      }
      brOut.write("GEDONE:"+geDone+"\n"); 
      brOut.close();      
      System.out.println("File stored successfully on HDFS!");   
                        
    } catch (Exception e) {
        e.printStackTrace();
    }
}
}
