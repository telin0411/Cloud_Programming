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
    Map<String, Double> base;
    public ValueComparator(Map<String, Double> base) {
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

public class rankCourse {
  public static void main(String[] args) throws IOException {

    // Initialize Config for table 1   
    Configuration config = HBaseConfiguration.create();
    config.set("hbase.master","localhost:60000");
    HBaseAdmin hbase = new HBaseAdmin(config);    
    if(hbase.tableExists(args[4])){
	 	  System.out.println("\nTable "+args[4]+" Exists!!");
    }   
    //HTable for courses with respect to the IDs
    HTable csCategory1 = new HTable(config, args[4]);
  
    // Initialize Config for table 2   
    Configuration config1 = HBaseConfiguration.create();
    config1.set("hbase.master","localhost:60000");
    HBaseAdmin hbase1 = new HBaseAdmin(config1);    
    if(hbase1.tableExists(args[5])){
	 	  System.out.println("\nTable "+args[5]+" Exists!!");
    }   
    //HTable for courses with respect to the IDs
    HTable csCategory2 = new HTable(config1, args[5]);
    
    // Initialize Config for table 3   
    Configuration config2 = HBaseConfiguration.create();
    config2.set("hbase.master","localhost:60000");
    HBaseAdmin hbase2 = new HBaseAdmin(config2);    
    if(hbase2.tableExists(args[6])){
	 	  System.out.println("\nTable "+args[6]+" Exists!!");
    }   
    //HTable for courses with respect to the IDs
    HTable csInfo1 = new HTable(config2, args[6]);
    
    // Initialize Config for table 4   
    Configuration config3 = HBaseConfiguration.create();
    config3.set("hbase.master","localhost:60000");
    HBaseAdmin hbase3 = new HBaseAdmin(config3);    
    if(hbase3.tableExists(args[7])){
	 	  System.out.println("\nTable "+args[7]+" Exists!!");
    }   
    //HTable for courses with respect to the IDs
    HTable csInfo2 = new HTable(config3, args[7]);   
    
    // Initialize Config for table 5   
    Configuration config4 = HBaseConfiguration.create();
    config4.set("hbase.master","localhost:60000");
    HBaseAdmin hbase4 = new HBaseAdmin(config4);    
    if(hbase4.tableExists(args[9])){
	 	  System.out.println("\nTable "+args[9]+" Exists!!");
    }   
    //HTable for courses with respect to the IDs
    HTable geInfo1 = new HTable(config4, args[9]);
    
    // Initialize Config for table 6   
    Configuration config5 = HBaseConfiguration.create();
    config5.set("hbase.master","localhost:60000");
    HBaseAdmin hbase5 = new HBaseAdmin(config5);    
    if(hbase5.tableExists(args[10])){
	 	  System.out.println("\nTable "+args[10]+" Exists!!");
    }   
    //HTable for courses with respect to the IDs
    HTable geInfo2 = new HTable(config5, args[10]);             

    System.out.println("\n********** The Rank Results **********");    

    // Array lists for query
    ArrayList<String> inListCStmp = new ArrayList<String>();
    ArrayList<String> inListGEtmp = new ArrayList<String>();
    ArrayList<String> inputGE = new ArrayList<String>();   
    ArrayList<String> inStuCourses = new ArrayList<String>(); 
    ArrayList<String> inStuCheck = new ArrayList<String>(); 

    // Hash map for categories
    HashMap<String, Double> csCategory = new HashMap<String, Double>();  
    HashMap<String, Integer> csCategorySet = new HashMap<String, Integer>();         
    csCategory.put("A", 1.0);
    csCategory.put("B", 2.0);
    csCategory.put("C", 3.0);
    csCategory.put("D", 48.0);
    csCategory.put("E", 18.0);  
    csCategorySet.put("A", 1);
    csCategorySet.put("B", 2);
    csCategorySet.put("C", 3);
    csCategorySet.put("D", 48);
    csCategorySet.put("E", 18);      
                         
    // Hash maps CS    
    HashMap<String, Integer> courseRankCS = new HashMap<String, Integer>();
    HashMap<Integer, String> courseCSIndex = new HashMap<Integer, String>();
    HashMap<String, Double> rateCSAvg = new HashMap<String, Double>();
    HashMap<String, Double> hardCSAvg = new HashMap<String, Double>();
    HashMap<String, Double> rankCSAvg = new HashMap<String, Double>();
    HashMap<String, Double> combCSAvg = new HashMap<String, Double>();
    ValueComparator vc =  new ValueComparator(combCSAvg);
    TreeMap<String, Double> sorted_combCS = new TreeMap<String, Double>(vc);
    
    // Hash maps GE  
    HashMap<String, Integer> courseRankGE = new HashMap<String, Integer>();
    HashMap<Integer, String> courseGEIndex = new HashMap<Integer, String>();
    HashMap<String, Double> rateGEAvg = new HashMap<String, Double>();
    HashMap<String, Double> hardGEAvg = new HashMap<String, Double>();
    HashMap<String, Double> rankGEAvg = new HashMap<String, Double>();
    HashMap<String, Double> combGEAvg = new HashMap<String, Double>();
    HashMap<String, Double> combGEAvg2 = new HashMap<String, Double>();    
    ValueComparator vc1 =  new ValueComparator(combGEAvg);
    ValueComparator vc2 =  new ValueComparator(combGEAvg2);    
    TreeMap<String, Double> sorted_combGE = new TreeMap<String, Double>(vc1); 
    TreeMap<String, Double> sorted_combGE2 = new TreeMap<String, Double>(vc2);       
    
    int i;
    int querySem=0; // the query semester
    // Import the students results from hdfs
    Path inStu = new Path(args[0]);
    FileSystem fsStu = FileSystem.get(new Configuration());
    FSDataInputStream fstreamStu = fsStu.open(inStu);
    BufferedReader brStu = new BufferedReader(new InputStreamReader(fstreamStu)); 
    String inStuTmp = new String();
    //Rearrange the output format
    int stuNum=0;    
    while(brStu.ready()){
      inStuTmp = brStu.readLine();
      System.out.println(inStuTmp);
      String student1[] = inStuTmp.split(";");
      querySem = Integer.parseInt(student1[0]);
      String student2[] = student1[1].split(",");
      stuNum = student2.length; 
    }       
    System.out.println("Students number = "+stuNum+" query Semester = "+querySem);
    fsStu.close();    
    
    // Import the ranking results CS from hdfs
    Path inList = new Path(args[1]);
    FileSystem fsIn = FileSystem.get(new Configuration());
    FSDataInputStream fstreamIn = fsIn.open(inList);
    BufferedReader brIn = new BufferedReader(new InputStreamReader(fstreamIn)); 
    String inTmp = new String();
    //Rearrange the output format
    while(brIn.ready()){
      inTmp = brIn.readLine();
      inListCStmp.add(inTmp); 
    }       
    fsIn.close();
    
    // Import the ranking results GE from hdfs
    Path inList2 = new Path(args[2]);
    FileSystem fsIn2 = FileSystem.get(new Configuration());
    FSDataInputStream fstreamIn2 = fsIn2.open(inList2);
    BufferedReader brIn2 = new BufferedReader(new InputStreamReader(fstreamIn2)); 
    String inTmp2 = new String();
    //Rearrange the output format
    while(brIn2.ready()){
      inTmp2 = brIn2.readLine();
      //System.out.println(inTmp);
      String test[] = inTmp2.split(":");
      if(test[0].equals("GEDONE") && test.length>2){
        String geTest[] = test[1].split(",");
        for(int k=0; k<geTest.length; k++){
          inputGE.add(geTest[k]);
        }
      }
      else{
        inListGEtmp.add(inTmp2); 
      }
    }       
    fsIn2.close();    
    System.out.println(inputGE);
    
    // Import the input students     
    BufferedReader brStuInfo = new BufferedReader(new FileReader(args[3]));       
    while(brStuInfo.ready()){
      inTmp = brStuInfo.readLine();
      String inStu1[] = inTmp.split("\t");
      String inStu2[] = inStu1[1].split(","); 
      for(int m=0; m<inStu2.length; m++){
        inStuCourses.add(inStu2[m]);
        if(!inStuCheck.contains(inStu2[m].substring(0,6))){
          inStuCheck.add(inStu2[m].substring(0,6));
        }
      } 
    }
    brStuInfo.close(); 
          
    try {
      
      String inCSTmp = new String();
      String inCSTmpOrg = new String();
      String valueStr = new String();
      String valueStr1 = new String();
      // Deal with the ABC categories and the requirements
      for(i=0; i<inStuCourses.size(); i++){
        inCSTmpOrg = inStuCourses.get(i);
        int inSem = Integer.parseInt(inCSTmpOrg.substring(inCSTmpOrg.length()-1));        
        inCSTmp = inStuCourses.get(i).substring(0,7);      
        if(inCSTmp.substring(0,2).equals("CS")){
          //System.out.print(inSem+";");
          if(inSem%2==1){
            //System.out.println(inCSTmp);
            Get get_data = new Get(Bytes.toBytes(inCSTmp)); // Course Categories
            Result res = csCategory1.get(get_data);  
            byte[] value = res.getValue(Bytes.toBytes("Course"), Bytes.toBytes("number"));
            valueStr = Bytes.toString(value);     
            Get get_data1 = new Get(Bytes.toBytes(inCSTmp)); // Course Information
            Result res1 = csInfo1.get(get_data1);  
            byte[] value1 = res1.getValue(Bytes.toBytes("Course"), Bytes.toBytes("number"));
            valueStr1 = Bytes.toString(value1);
            String crStr[] = valueStr1.split(";"); // Parse the return string to get the credits
            double credit = Double.parseDouble(crStr[2]);
            //System.out.print("Credit = "+credit+";");        
            if(valueStr.equals("A") || valueStr.equals("B") || valueStr.equals("C")){
              double catNum = csCategory.get(valueStr);
              if(catNum > 0){
                catNum-=1.0;
              }
              csCategory.put(valueStr, catNum);
            } 
            else if(valueStr.equals("D") || valueStr.equals("E")){
              double catNum = csCategory.get(valueStr);
              if(catNum > 0){
                catNum-=credit;
              }
              csCategory.put(valueStr, catNum);
            }                  
          }
          else{
            Get get_data = new Get(Bytes.toBytes(inCSTmp)); // Course Categories
            Result res = csCategory2.get(get_data);  
            byte[] value = res.getValue(Bytes.toBytes("Course"), Bytes.toBytes("number"));
            valueStr = Bytes.toString(value);   
            Get get_data1 = new Get(Bytes.toBytes(inCSTmp)); // Course Information
            Result res1 = csInfo2.get(get_data1);  
            byte[] value1 = res1.getValue(Bytes.toBytes("Course"), Bytes.toBytes("number"));
            valueStr1 = Bytes.toString(value1);
            String crStr[] = valueStr1.split(";"); // Parse the return string to get the credits
            int credit = Integer.parseInt(crStr[2]); 
            //System.out.print("Credit = "+credit+";"); 
            if(valueStr.equals("A") || valueStr.equals("B") || valueStr.equals("C")){
              double catNum = csCategory.get(valueStr);
              if(catNum > 0){
                catNum-=1.0;
              }
              csCategory.put(valueStr, catNum);  
            } 
            else if(valueStr.equals("D") || valueStr.equals("E")){
              double catNum = csCategory.get(valueStr);
              if(catNum > 0){
                catNum-=credit;
              }
              csCategory.put(valueStr, catNum);
            }                                
          }
          //System.out.println(inCSTmp+";"+valueStr);              
        }
      }
      System.out.println(csCategory);
      
      // Deal with CS coueses     
      for(i=0; i<inListCStmp.size(); i++){
        String strList[] = inListCStmp.get(i).split(";");
        String strTmp1[] = strList[0].split(":");
        double ranking = Double.parseDouble(strTmp1[1]) / stuNum * 100;
        rankCSAvg.put(strTmp1[0], ranking);
        String strTmp2[] = strList[1].split("=");
        double ratings = Double.parseDouble(strTmp2[1]) / 9 * 100;
        rateCSAvg.put(strTmp1[0], ratings);       
        String strTmp3[] = strList[2].split("=");
        double hardness = Double.parseDouble(strTmp3[1]);
        hardCSAvg.put(strTmp1[0], hardness);        
        //Deal with the categories for CS courses
        //System.out.println(strTmp1[0]);
        if(querySem%2==1){
          Get get_data = new Get(Bytes.toBytes(strTmp1[0])); // Course Categories
          Result res = csCategory1.get(get_data);  
          byte[] value = res.getValue(Bytes.toBytes("Course"), Bytes.toBytes("number"));
          valueStr = Bytes.toString(value);
        }    
        else{
          Get get_data = new Get(Bytes.toBytes(strTmp1[0])); // Course Categories
          Result res = csCategory2.get(get_data);  
          byte[] value = res.getValue(Bytes.toBytes("Course"), Bytes.toBytes("number"));
          valueStr = Bytes.toString(value); 
        }
        //System.out.println(valueStr);
        double csDataCat = csCategory.get(valueStr);
        double csDataCatVal = csDataCat / csCategorySet.get(valueStr) * 100;       
        //System.out.println("Courses="+strTmp1[0]+" category="+valueStr+" value="+csDataCatVal);
        // combine the ratings and the rankings
        double combines = ranking + ratings + csDataCatVal;
        combCSAvg.put(strTmp1[0], combines);
      }   
      
      sorted_combCS.putAll(combCSAvg);
      for(Object key:sorted_combCS.keySet()) {
        //System.out.println(key+":"+combCSAvg.get(key));
      }      
      
      System.out.println(inListGEtmp);      
      // Deal with GE coueses
      for(i=0; i<inListGEtmp.size()-1; i++){
        String strListGE[] = inListGEtmp.get(i).split(";");
        String strTmpGE1[] = strListGE[0].split(":");
        double rankingGE = Double.parseDouble(strTmpGE1[1]) / stuNum * 100;
        rankGEAvg.put(strTmpGE1[0], rankingGE);
        String strTmpGE2[] = strListGE[1].split("=");
        double ratingsGE = Double.parseDouble(strTmpGE2[1]) / 9 * 100;
        rateGEAvg.put(strTmpGE1[0], ratingsGE);       
        String strTmpGE3[] = strListGE[2].split("=");
        double hardnessGE = Double.parseDouble(strTmpGE3[1]);
        hardGEAvg.put(strTmpGE1[0], hardnessGE);
        // combine the ratings and the rankings
        
        // Deal with the portion of the students selecting the courses
        int currentSem = Integer.parseInt(strTmpGE1[0].substring(3,4));
        currentSem = currentSem % 2;
        //System.out.println(strTmpGE1[0]);
        if(currentSem % 2 == 1){
          Get get_data = new Get(Bytes.toBytes(strTmpGE1[0])); // Course Categories
          Result res = geInfo1.get(get_data);  
          byte[] value = res.getValue(Bytes.toBytes("Course"), Bytes.toBytes("number"));
          valueStr = Bytes.toString(value);         
        }
        else{
          Get get_data = new Get(Bytes.toBytes(strTmpGE1[0])); // Course Categories
          Result res = geInfo2.get(get_data);  
          byte[] value = res.getValue(Bytes.toBytes("Course"), Bytes.toBytes("number"));
          valueStr = Bytes.toString(value);        
        }        
        System.out.println(strTmpGE1[0]+":"+valueStr);
        String SL[] = valueStr.split(";");
        double numSelect = Double.parseDouble(SL[4]);
        double numLimit = Double.parseDouble(SL[5]);
        System.out.println(strTmpGE1[0]+":"+numSelect+"/"+numLimit);
        
        double combinesGE = rankingGE + ratingsGE;
        if(!inputGE.contains(strTmpGE1[0].substring(2,3))){
          combGEAvg.put(strTmpGE1[0], combinesGE);
        }
        else{
          combGEAvg2.put(strTmpGE1[0], combinesGE);  
        }
      }   
      //System.out.println(combGEAvg);
      sorted_combGE.putAll(combGEAvg);
      sorted_combGE2.putAll(combGEAvg2);      
      for(Object key:sorted_combGE.keySet()) {
        System.out.println(key+":"+combGEAvg.get(key));
      }        
      for(Object key:sorted_combGE2.keySet()) {
        System.out.println(key+":"+combGEAvg2.get(key));
      }         

      // Filing the output
      String tmp1 = new String();
      Configuration confOut = new Configuration();
      FileSystem fsOut = FileSystem.get(confOut);
      Path outFile = new Path(args[8]);
      FSDataOutputStream out = fsOut.create(outFile);
      BufferedWriter brOut = new BufferedWriter(new OutputStreamWriter(out)); 
      for(Object key:sorted_combCS.keySet()) {
        tmp1 = key.toString().substring(0,6); 
        if(!inStuCheck.contains(tmp1)){       
          brOut.write(key+":"+hardCSAvg.get(key)+"\n");
        }
      }
      for(Object key:sorted_combGE.keySet()) {
        tmp1 = key.toString().substring(0,6); 
        if(!inStuCheck.contains(tmp1)){       
          brOut.write(key+":"+hardGEAvg.get(key)+"\n");
        }
      }        
      for(Object key:sorted_combGE2.keySet()) {
        tmp1 = key.toString().substring(0,6); 
        if(!inStuCheck.contains(tmp1)){       
          brOut.write(key+":"+hardGEAvg.get(key)+"\n");
        }
      }        
      brOut.close();      
      System.out.println("File stored successfully on HDFS!");       
                              
    } catch (Exception e) {
        e.printStackTrace();
    }
}
}
