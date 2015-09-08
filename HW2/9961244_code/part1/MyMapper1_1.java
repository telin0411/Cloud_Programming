package hw2;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;
import java.util.ArrayList;

public class MyMapper1_1 extends MapReduceBase
	implements Mapper<Object, Text, Text, Text> {

  private final IntWritable one = new IntWritable(1);
  private Text word = new Text();
  private Text outVal = new Text(); 
  public ArrayList<String> titleList = new ArrayList<String>();
  public ArrayList<String> nodeList = new ArrayList<String>();  
  
  @Override
  public void configure(JobConf job){
    String titleStr = new String();
    String nodeStr = new String();
    titleStr = job.get("titleList");
    String tstr[] =  titleStr.split("]");
    int i;
    for(i=0; i<tstr.length; i++){
      titleList.add(tstr[i]);
    }
    nodeStr = job.get("nodeList");
    String nstr[] =  nodeStr.split("]");
    for(i=0; i<nstr.length; i++){
      if(!nodeList.contains(nstr[i])){
        nodeList.add(nstr[i]);
      }
    }    
  }  
  public void map(Object key, Text value,
      OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
   
		try{
      String lineOffset = key.toString();          
      //TODO eliminate the useless characters
      String inputStr = value.toString();
      String str1[] = inputStr.split("\t");
      String str2[] = str1[1].split("1");
      int i;
      // judge title if it's a dangling node
      if(str2[1].equals("]")){
        if(nodeList.contains(str1[0])){
          word.set("nullNode");
          outVal.set(str1[0]);
        }
        else{
          word.set(str1[0]);
          outVal.set("1]");
        }
      }
      else{  
        //judge outlink
        String str3[] = str2[1].split("]");        
        String outValTmp = new String();
        outValTmp = "";
        for(i=1; i<str3.length; i++){
          if(titleList.contains(str3[i])){
            outValTmp = outValTmp + str3[i] + "]";
          }
        }
        word.set(str1[0]); 
        outVal.set("1]"+outValTmp);
      }           
      output.collect(word, outVal);
		}catch(Exception e){
			e.printStackTrace();
			System.out.printf("inputValue = %s\n",value.toString());
		}    
  }
}
