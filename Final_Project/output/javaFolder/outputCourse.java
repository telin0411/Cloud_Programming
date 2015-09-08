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

public class outputCourse {
  public static void main(String[] args) throws IOException {

    // Initialize Config for table 1   
    Configuration config = HBaseConfiguration.create();
    config.set("hbase.master","localhost:60000");
    HBaseAdmin hbase = new HBaseAdmin(config);    
    if(hbase.tableExists(args[2])){
	 	  System.out.println("\nTable "+args[2]+" Exists!!");
    }   
    //HTable for courses with respect to the IDs
    HTable csCategory1 = new HTable(config, args[2]);
  
    // Initialize Config for table 2   
    Configuration config1 = HBaseConfiguration.create();
    config1.set("hbase.master","localhost:60000");
    HBaseAdmin hbase1 = new HBaseAdmin(config1);    
    if(hbase1.tableExists(args[3])){
	 	  System.out.println("\nTable "+args[3]+" Exists!!");
    }   
    //HTable for courses with respect to the IDs
    HTable csCategory2 = new HTable(config1, args[3]);
    
    // Initialize Config for table 3   
    Configuration config2 = HBaseConfiguration.create();
    config2.set("hbase.master","localhost:60000");
    HBaseAdmin hbase2 = new HBaseAdmin(config2);    
    if(hbase2.tableExists(args[4])){
	 	  System.out.println("\nTable "+args[4]+" Exists!!");
    }   
    //HTable for courses with respect to the IDs
    HTable csInfo1 = new HTable(config2, args[4]);
    
    // Initialize Config for table 4   
    Configuration config3 = HBaseConfiguration.create();
    config3.set("hbase.master","localhost:60000");
    HBaseAdmin hbase3 = new HBaseAdmin(config3);    
    if(hbase3.tableExists(args[5])){
	 	  System.out.println("\nTable "+args[5]+" Exists!!");
    }   
    //HTable for courses with respect to the IDs
    HTable csInfo2 = new HTable(config3, args[5]);      
    
    // Initialize Config for table 4   
    Configuration config4 = HBaseConfiguration.create();
    config4.set("hbase.master","localhost:60000");
    HBaseAdmin hbase4 = new HBaseAdmin(config4);    
    if(hbase3.tableExists(args[8])){
	 	  System.out.println("\nTable "+args[8]+" Exists!!");
    }   
    //HTable for courses with respect to the IDs
    HTable sortByClass = new HTable(config4, args[8]);     
    
    // Initialize Config for table 5   
    Configuration config5 = HBaseConfiguration.create();
    config5.set("hbase.master","localhost:60000");
    HBaseAdmin hbase5 = new HBaseAdmin(config5);    
    if(hbase5.tableExists(args[11])){
	 	  System.out.println("\nTable "+args[11]+" Exists!!");
    }   
    //HTable for courses with respect to the IDs
    HTable geInfo1 = new HTable(config5, args[11]);
    
    // Initialize Config for table 6   
    Configuration config6 = HBaseConfiguration.create();
    config6.set("hbase.master","localhost:60000");
    HBaseAdmin hbase6 = new HBaseAdmin(config6);    
    if(hbase6.tableExists(args[12])){
	 	  System.out.println("\nTable "+args[12]+" Exists!!");
    }   
    //HTable for courses with respect to the IDs
    HTable geInfo2 = new HTable(config6, args[12]);        

    System.out.println("\n********** The Rank Results **********");    

    // Array lists for query
    ArrayList<String> inListCS = new ArrayList<String>();
    ArrayList<String> inListGE = new ArrayList<String>(); 
    ArrayList<String> inputGE = new ArrayList<String>();
    ArrayList<String> inStuCourses = new ArrayList<String>(); 
    ArrayList<String> inStuCheck = new ArrayList<String>();    
    ArrayList<String> totalCS = new ArrayList<String>();    
    ArrayList<String> totalGE = new ArrayList<String>();
    ArrayList<String> rankedCS = new ArrayList<String>();
    ArrayList<String> rankedGE = new ArrayList<String>();   
    ArrayList<String> failedCS = new ArrayList<String>();      

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
    ValueComparator vc =  new ValueComparator(rateCSAvg);
    TreeMap<String, Double> sorted_rateCS = new TreeMap<String, Double>(vc);
    HashMap<String, Double> inrankCS = new HashMap<String, Double>(); 
    
    // Hash maps GE  
    HashMap<String, Integer> courseRankGE = new HashMap<String, Integer>();
    HashMap<Integer, String> courseGEIndex = new HashMap<Integer, String>();
    HashMap<String, Double> rateGEAvg = new HashMap<String, Double>();
    HashMap<String, Double> rateGEAvg2 = new HashMap<String, Double>();
    HashMap<String, Double> hardGEAvg = new HashMap<String, Double>();
    HashMap<String, Double> hardGEAvg2 = new HashMap<String, Double>();    
    HashMap<String, Double> rankGEAvg = new HashMap<String, Double>();     
    ValueComparator vc1 =  new ValueComparator(rateGEAvg); 
    ValueComparator vc2 =  new ValueComparator(rateGEAvg2);    
    TreeMap<String, Double> sorted_rateGE = new TreeMap<String, Double>(vc1);      
    TreeMap<String, Double> sorted_rateGE2 = new TreeMap<String, Double>(vc2);
    HashMap<String, Double> inrankGE = new HashMap<String, Double>();       
    
    // Strings required
    String tlCSTmp = new String();
    String tlGETmp = new String();
    String valueStr = new String();
    String valueStr1 = new String();
    String inCSTmp = new String();
    String inCSTmpOrg = new String(); 
    
    int i;
    String inTmp = new String();    
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
    
    // Import the input student
    BufferedReader brStuInfo = new BufferedReader(new FileReader(args[1]));       
    while(brStuInfo.ready()){
      inTmp = brStuInfo.readLine();
      String inStu1[] = inTmp.split("\t");
      String inStu2[] = inStu1[1].split(",");
      for(int m=0; m<inStu2.length; m++){
        String inStu3[] = inStu2[m].split(";"); 
        int currentSem = Integer.parseInt(inStu3[0].substring(inStu3[0].length()-1));    
        int passFail = Integer.parseInt(inStu3[1]); 
        if(!inStuCourses.contains(inStu3[0])){
          inStuCourses.add(inStu3[0]);
        }
        if(passFail!=0 && failedCS.contains(inStu3[0].substring(0,6))){
          failedCS.remove(inStu3[0].substring(0,6));
        }
        else if(!inStuCheck.contains(inStu3[0].substring(0,6)) && passFail!=0){
          inStuCheck.add(inStu3[0].substring(0,6));        
        }
        else if(!inStuCheck.contains(inStu3[0].substring(0,6)) && passFail==0){
          if(currentSem%2==1){
            Get get_data = new Get(Bytes.toBytes(inStu3[0].substring(0,7))); // Course Categories
            Result res = csCategory1.get(get_data);  
            byte[] value = res.getValue(Bytes.toBytes("Course"), Bytes.toBytes("number"));
            valueStr = Bytes.toString(value);              
          }
          else{
            Get get_data = new Get(Bytes.toBytes(inStu3[0].substring(0,7))); // Course Categories
            Result res = csCategory2.get(get_data);  
            byte[] value = res.getValue(Bytes.toBytes("Course"), Bytes.toBytes("number"));
            valueStr = Bytes.toString(value);            
          }
          System.out.print(inStu3[0].substring(0,7));
          System.out.println(valueStr);
          if(valueStr.equals("D")){
            failedCS.add(inStu3[0].substring(0,6));
          }
        }
        //System.out.println(currentSem+":"+inStu3[0].substring(0,6)+":"+passFail);
      } 
    }
    System.out.println(failedCS);
    brStuInfo.close(); 
    
    // Import the ranked results 
    Path inRank = new Path(args[7]);
    FileSystem fsRank = FileSystem.get(new Configuration());
    FSDataInputStream fstreamRank = fsRank.open(inRank);
    BufferedReader brRank = new BufferedReader(new InputStreamReader(fstreamRank));     
    while(brRank.ready()){
      inTmp = brRank.readLine();
      String inRank1[] = inTmp.split(":"); 
      double hardness = Double.parseDouble(inRank1[1]);
      if(inRank1[0].substring(0,2).equals("GE")){
        rankedGE.add(inRank1[0]);
        inrankGE.put(inRank1[0], hardness);      
      }
      else{
        rankedCS.add(inRank1[0]); 
        inrankCS.put(inRank1[0], hardness);          
      }         
    }
    brRank.close();

    // Import the sortByClass map
    Path inMap = new Path(args[6]);
    FileSystem fsMap = FileSystem.get(new Configuration());
    FSDataInputStream fstreamMap = fsMap.open(inMap);
    BufferedReader brMap = new BufferedReader(new InputStreamReader(fstreamMap));        
    while(brMap.ready()){
      inTmp = brMap.readLine();
      String inMap1[] = inTmp.split("\t");
      String inMap2[] = inMap1[1].split(";"); 
      if(inMap1[0].substring(0,2).equals("GE")){
        totalGE.add(inMap1[0]);      
      }
      else{
        totalCS.add(inMap1[0]);
      }
    }
    brMap.close();
    
    // Import the ranking results GE from hdfs
    Path inList2 = new Path(args[9]);
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
    }       
    fsIn2.close();    
    System.out.println(inputGE);   
          
    try {      
    
      for(i=0; i<inStuCourses.size(); i++){
        inCSTmpOrg = inStuCourses.get(i);
        int inSem = Integer.parseInt(inCSTmpOrg.substring(inCSTmpOrg.length()-1));        
        inCSTmp = inStuCourses.get(i).substring(0,7);      
        if(inCSTmp.substring(0,2).equals("CS")){
          //System.out.print(inSem+";");
          if(inSem%2==1){
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
        }
      }
      System.out.println(csCategory);
      
      // Output the CS Courses          
      for(i=0; i<totalCS.size(); i++){
        tlCSTmp = totalCS.get(i);
        int inputSem = Integer.parseInt(tlCSTmp.substring(3,4));
        if(inputSem==querySem%2 && !rankedCS.contains(tlCSTmp) && !inStuCheck.contains(tlCSTmp.substring(0,6))){
          if(querySem%2==1){
            Get get_data = new Get(Bytes.toBytes(tlCSTmp)); // Course Categories
            Result res = csCategory1.get(get_data);  
            byte[] value = res.getValue(Bytes.toBytes("Course"), Bytes.toBytes("number"));
            valueStr = Bytes.toString(value);   
          }    
          else{
            Get get_data = new Get(Bytes.toBytes(tlCSTmp)); // Course Categories
            Result res = csCategory2.get(get_data);  
            byte[] value = res.getValue(Bytes.toBytes("Course"), Bytes.toBytes("number"));
            valueStr = Bytes.toString(value);    
          }      
          double csDataCat = csCategory.get(valueStr);
          double csDataCatVal = csDataCat / csCategorySet.get(valueStr) * 100;
          Get get_data = new Get(Bytes.toBytes(tlCSTmp)); // Course Categories
          Result res = sortByClass.get(get_data);  
          byte[] value = res.getValue(Bytes.toBytes("ID"), Bytes.toBytes("Avg"));
          valueStr = Bytes.toString(value); 
          String RH[] = valueStr.split(";");
          //System.out.println(tlCSTmp+":"+csDataCatVal+":"+RH[1]+":"+RH[0]);
          double inRate = Double.parseDouble(RH[1]);
          double inHard = Double.parseDouble(RH[0]);
          inRate += csDataCatVal;
          rateCSAvg.put(tlCSTmp, inRate);
          hardCSAvg.put(tlCSTmp, inHard);                                       
        }                
      }
      
      // Output the GE Courses          
      for(i=0; i<totalGE.size(); i++){
        tlGETmp = totalGE.get(i);
        int inputSem = Integer.parseInt(tlGETmp.substring(3,4));
        if(inputSem==querySem%2 && !rankedGE.contains(tlGETmp) && !inStuCheck.contains(tlGETmp.substring(0,6))){    
          Get get_data = new Get(Bytes.toBytes(tlGETmp)); // Course Categories
          Result res = sortByClass.get(get_data);  
          byte[] value = res.getValue(Bytes.toBytes("ID"), Bytes.toBytes("Avg"));
          valueStr = Bytes.toString(value); 
          String RH[] = valueStr.split(";");
          //System.out.println(tlCSTmp+":"+csDataCatVal+":"+RH[1]+":"+RH[0]);
          double inRate = Double.parseDouble(RH[1]);
          double inHard = Double.parseDouble(RH[0]);  
          if(!inputGE.contains(tlGETmp.substring(2,3))){
            rateGEAvg.put(tlGETmp, inRate);
            hardGEAvg.put(tlGETmp, inHard);
          }
          else{
            rateGEAvg2.put(tlGETmp, inRate);
            hardGEAvg2.put(tlGETmp, inHard);   
          }                                               
        }                
      }      

      // File out
      int judgeFail = 0;
      Configuration confOut = new Configuration();
      FileSystem fsOut = FileSystem.get(confOut);
      Path outFile = new Path(args[10]);
      FSDataOutputStream out = fsOut.create(outFile);
      BufferedWriter brOut = new BufferedWriter(new OutputStreamWriter(out));
      if(querySem%2==0){
        querySem = 2;
      }
      else{
        querySem = 1;
      }
      brOut.write(querySem+"\n");
     
      NumberFormat nf = NumberFormat.getInstance();
      nf.setMinimumFractionDigits(2);
      nf.setMaximumFractionDigits(2);
      nf.setGroupingUsed(false);      
      // Sort the CS Courses
      sorted_rateCS.putAll(rateCSAvg);
      
      // Failed courses of CS must
      for(i=0; i<failedCS.size(); i++){
        for(int k=1; k<5; k++){
          Get get_dataFail = new Get(Bytes.toBytes(failedCS.get(i)+k)); // Course Categories
          Result resFail = sortByClass.get(get_dataFail);  
          byte[] valueFail = resFail.getValue(Bytes.toBytes("ID"), Bytes.toBytes("Avg"));
          valueStr = Bytes.toString(valueFail);
          if(valueStr!=null){
            judgeFail = 1;
            String RH[] = valueStr.split(";");
            double inRate = Double.parseDouble(RH[1]);
            double inHard = Double.parseDouble(RH[0]);
            brOut.write(failedCS.get(i)+k+":"+nf.format(inHard)+":"+judgeFail+"\n");            
          }
        }        
      }
      
      judgeFail = 0;
      for(i=0; i<rankedCS.size(); i++){
        //System.out.println(rankedCS.get(i)+":"+inrankCS.get(rankedCS.get(i)));
        if(!failedCS.contains(rankedCS.get(i).substring(0,6))){
          brOut.write(rankedCS.get(i)+":"+inrankCS.get(rankedCS.get(i))+":"+judgeFail+"\n");
        }
      }
      for(Object key:sorted_rateCS.keySet()) {
        //System.out.println(key+":"+nf.format(hardCSAvg.get(key)));
        if(!failedCS.contains(key.toString().substring(0,6))){        
          brOut.write(key+":"+nf.format(hardCSAvg.get(key))+":"+judgeFail+"\n");
        }
      }       
      
      // Sort the GE Courses
      sorted_rateGE.putAll(rateGEAvg);
      for(i=0; i<rankedGE.size(); i++){
        Get get_data = new Get(Bytes.toBytes(rankedGE.get(i))); // Course Categories
        Result res = sortByClass.get(get_data);  
        byte[] value = res.getValue(Bytes.toBytes("ID"), Bytes.toBytes("Avg"));
        valueStr = Bytes.toString(value); 
        String RH[] = valueStr.split(";");
        double inRate = Double.parseDouble(RH[1]);
        double inHard = Double.parseDouble(RH[0]);
        if(querySem % 2 == 1){
          Get get_datag = new Get(Bytes.toBytes(rankedGE.get(i))); // Course Categories
          Result resg = geInfo1.get(get_datag);  
          byte[] valueg = resg.getValue(Bytes.toBytes("Course"), Bytes.toBytes("number"));
          valueStr = Bytes.toString(valueg);         
        }
        else{
          Get get_datag = new Get(Bytes.toBytes(rankedGE.get(i))); // Course Categories
          Result resg = geInfo2.get(get_datag);  
          byte[] valueg = resg.getValue(Bytes.toBytes("Course"), Bytes.toBytes("number"));
          valueStr = Bytes.toString(valueg);     
        }
        String SL[] = valueStr.split(";");
        double numSelect = Double.parseDouble(SL[4]);
        double numLimit = Double.parseDouble(SL[5]);
        double ratio = numSelect / numLimit;        
        if(!inputGE.contains(rankedGE.get(i).substring(2,3))){
          //System.out.println(rankedGE.get(i)+":"+nf.format(inHard));
          if(ratio > 2){
            brOut.write(rankedGE.get(i)+":"+nf.format(inHard)+":"+"2"+"\n");
          }
          else{
            brOut.write(rankedGE.get(i)+":"+nf.format(inHard)+":"+judgeFail+"\n");  
          }
        }
      }      
      for(Object key:sorted_rateGE.keySet()) {
        //System.out.println(key+":"+nf.format(hardGEAvg.get(key)));
        if(querySem % 2 == 1){
          Get get_datag = new Get(Bytes.toBytes(key.toString())); // Course Categories
          Result resg = geInfo1.get(get_datag);  
          byte[] valueg = resg.getValue(Bytes.toBytes("Course"), Bytes.toBytes("number"));
          valueStr = Bytes.toString(valueg);         
        }
        else{
          Get get_datag = new Get(Bytes.toBytes(key.toString())); // Course Categories
          Result resg = geInfo2.get(get_datag);  
          byte[] valueg = resg.getValue(Bytes.toBytes("Course"), Bytes.toBytes("number"));
          valueStr = Bytes.toString(valueg);     
        }
        String SL[] = valueStr.split(";");
        double numSelect = Double.parseDouble(SL[4]);
        double numLimit = Double.parseDouble(SL[5]);
        double ratio = numSelect / numLimit;
        if(ratio > 2){
          brOut.write(rankedGE.get(i)+":"+nf.format(hardGEAvg.get(key))+":"+"2"+"\n");
        }
        else{
          brOut.write(rankedGE.get(i)+":"+nf.format(hardGEAvg.get(key))+":"+judgeFail+"\n");  
        }
      }
      sorted_rateGE2.putAll(rateGEAvg2);
      for(i=0; i<rankedGE.size(); i++){
        Get get_data = new Get(Bytes.toBytes(rankedGE.get(i))); // Course Categories
        Result res = sortByClass.get(get_data);  
        byte[] value = res.getValue(Bytes.toBytes("ID"), Bytes.toBytes("Avg"));
        valueStr = Bytes.toString(value); 
        String RH[] = valueStr.split(";");
        double inRate = Double.parseDouble(RH[1]);
        double inHard = Double.parseDouble(RH[0]);
        if(querySem % 2 == 1){
          Get get_datag = new Get(Bytes.toBytes(rankedGE.get(i))); // Course Categories
          Result resg = geInfo1.get(get_datag);  
          byte[] valueg = resg.getValue(Bytes.toBytes("Course"), Bytes.toBytes("number"));
          valueStr = Bytes.toString(valueg);         
        }
        else{
          Get get_datag = new Get(Bytes.toBytes(rankedGE.get(i))); // Course Categories
          Result resg = geInfo2.get(get_datag);  
          byte[] valueg = resg.getValue(Bytes.toBytes("Course"), Bytes.toBytes("number"));
          valueStr = Bytes.toString(valueg);     
        }
        String SL[] = valueStr.split(";");
        double numSelect = Double.parseDouble(SL[4]);
        double numLimit = Double.parseDouble(SL[5]);
        double ratio = numSelect / numLimit;        
        if(inputGE.contains(rankedGE.get(i).substring(2,3))){
          //System.out.println(rankedGE.get(i)+":"+nf.format(inHard));
          if(ratio > 2){
            brOut.write(rankedGE.get(i)+":"+nf.format(inHard)+":"+"2"+"\n");
          }
          else{
            brOut.write(rankedGE.get(i)+":"+nf.format(inHard)+":"+judgeFail+"\n");  
          }
        }
      }       
      for(Object key:sorted_rateGE2.keySet()) {
        //System.out.println(key+":"+nf.format(hardGEAvg2.get(key)));
        if(querySem % 2 == 1){
          Get get_datag = new Get(Bytes.toBytes(key.toString())); // Course Categories
          Result resg = geInfo1.get(get_datag);  
          byte[] valueg = resg.getValue(Bytes.toBytes("Course"), Bytes.toBytes("number"));
          valueStr = Bytes.toString(valueg);         
        }
        else{
          Get get_datag = new Get(Bytes.toBytes(key.toString())); // Course Categories
          Result resg = geInfo2.get(get_datag);  
          byte[] valueg = resg.getValue(Bytes.toBytes("Course"), Bytes.toBytes("number"));
          valueStr = Bytes.toString(valueg);     
        }
        String SL[] = valueStr.split(";");
        double numSelect = Double.parseDouble(SL[4]);
        double numLimit = Double.parseDouble(SL[5]);
        double ratio = numSelect / numLimit;
        if(ratio > 2){
          brOut.write(rankedGE.get(i)+":"+nf.format(hardGEAvg2.get(key))+":"+"2"+"\n");
        }
        else{
          brOut.write(rankedGE.get(i)+":"+nf.format(hardGEAvg2.get(key))+":"+judgeFail+"\n");  
        }
      }                   
      brOut.close(); // close the file
                     
    } catch (Exception e) {
        e.printStackTrace();
    }
}
}
