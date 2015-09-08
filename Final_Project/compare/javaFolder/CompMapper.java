package fp;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;
import java.util.ArrayList;
import java.lang.Math.*;

public class CompMapper extends MapReduceBase
	implements Mapper<Object, Text, Text, Text> {

  private final IntWritable one = new IntWritable(1);
  private Text word = new Text();
  private Text outVal = new Text(); 
  HashMap<Integer, String> inputData = new HashMap<Integer, String>(); 
  HashMap<String, Integer> inputDataRate = new HashMap<String, Integer>(); 
  HashMap<String, Integer> inputDataHard = new HashMap<String, Integer>();     
  HashMap<String, Integer> inputData1 = new HashMap<String, Integer>();
  HashMap<Integer, String> courseTmp = new HashMap<Integer, String>();
  public static int lastSem;
    
  @Override
  public void configure(JobConf job){
    String inputStr = new String();
    inputStr = job.get("inputStudent");
    String inStu[] = inputStr.split(",");
    int i;
    int semester;
    int reTake = 1;
    int rate, hardness;
    for(i=0; i<inStu.length; i++){
      if(!inputData.containsValue(inStu[i].substring(0,7))){    
        inputData.put(i, inStu[i].substring(0,7));
        semester = Integer.parseInt(inStu[i].substring(inStu[i].length()-1));     
        inputData1.put(inStu[i].substring(0,7), semester);
        rate = Integer.parseInt(inStu[i].substring(8,9));
        hardness = Integer.parseInt(inStu[i].substring(7,8));
        inputDataRate.put(inStu[i].substring(0,7), rate);
        inputDataHard.put(inStu[i].substring(0,7), hardness);        
      }
      else{
        //inputData.put(i, inStu[i].substring(0,6)+"R"+reTake);
        semester = Integer.parseInt(inStu[i].substring(inStu[i].length()-1));     
        inputData1.put(inStu[i].substring(0,7)+"R"+reTake, semester); 
        rate = Integer.parseInt(inStu[i].substring(8,9));
        hardness = Integer.parseInt(inStu[i].substring(7,8));
        inputDataRate.put(inStu[i].substring(0,7), rate);
        inputDataHard.put(inStu[i].substring(0,7), hardness);         
        reTake++;    
      } 
    }
    int LStmp = inStu.length-1;
    int LS = Integer.parseInt(inStu[LStmp].substring(inStu[LStmp].length()-1));
    lastSem = LS;
  }
    
  public void map(Object key, Text value,
      OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
   
		try{
      String lineOffset = key.toString();          
      //TODO eliminate the useless characters
      String inputKey[] = value.toString().split("\t");
      String inputValue[] = inputKey[1].split(",");
      String outValStr = new String();
      int i;
      int semData; // the semester index of the data in the database
      int semInput; // the semester index of the input student
      int courseNum; // the number of courses till the last semester with repect to the input student
      int RE;
      double simVal; // total similarities
      double simValSem; // similarities with respect to semester
      double diffRate; // similarities with repect to rate
      double diffHard; // similarities with respect to hardness
      simVal = 0.0;
      simValSem = 0.0;
      courseNum = 0;
      String courses = new String();
      courses = "";
      RE = 1;
      for(i=0; i<inputValue.length; i++){
        int LStmp = Integer.parseInt(inputValue[i].substring(inputValue[i].length()-1));
        //if(LStmp<=lastSem){
        if(LStmp<=lastSem && inputValue[i].substring(0,2).equals("CS")){
          courseNum++;
          if(inputData.containsValue(inputValue[i].substring(0,7))){    
            int rateIn, hardIn, rateData, hardData;
            rateIn = inputDataRate.get(inputValue[i].substring(0,7));
            hardIn = inputDataHard.get(inputValue[i].substring(0,7));
            rateData = Integer.parseInt(inputValue[i].substring(8,9));
            hardData = Integer.parseInt(inputValue[i].substring(7,8));
            // Determine the similarities between the ratings and the hardness
            diffRate = (10 - Math.abs(rateIn - rateData)) / 10.0;
            diffHard = (10 - Math.abs(hardIn - hardData)) / 10.0;                
            //simVal += (1.0 * diffRate);
            simVal += 1.0;
            semData = Integer.parseInt(inputValue[i].substring(inputValue[i].length()-1)); 
            //if(!courseTmp.containsValue(inputValue[i].substring(0,6))){
            semInput = inputData1.get(inputValue[i].substring(0,7));            
            //courseTmp.put(i, inputValue[i].substring(0,6));
            if(semInput == semData){
              //simValSem += (1.0 * diffRate);  
              simValSem += 1.0;
            }             
                              
            //courses = courses + inputValue[i].substring(0,6) + ",";
            //}
            /*else if(courseTmp.containsValue(inputValue[i].substring(0,6))){          
              semData = inputData1.get(inputValue[i].substring(0,6)+"R"+RE);
              courseTmp.put(i, inputValue[i].substring(0,6)+"R"+RE);       
              RE++;
              if(semInput == semData){
                simValSem += 1.0;
              }       
            }*/
          }
        }
      }    
      //courseTmp.clear();
      simVal = simVal / courseNum * 100;
      simValSem = simValSem / courseNum * 100;      
      outValStr = "" + simVal;
      outVal.set(outValStr);    
      word.set(inputKey[0]+"<<"+simValSem+">>");       
      output.collect(outVal, word);
		}catch(Exception e){
			e.printStackTrace();
			System.out.printf("inputValue = %s\n",value.toString());
		}    
  }
}
