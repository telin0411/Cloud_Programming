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

public class MyMapper2 extends MapReduceBase
	implements Mapper<Object, Text, Text, Text> {

  private final IntWritable one = new IntWritable(1);
  private Text word = new Text();
  private Text outVal = new Text(); 
  
  @Override
  public void map(Object key, Text value,
      OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      
    /*String patternStr = "<title>[\\w+\\s+]+";
    Pattern pattern = Pattern.compile(patternStr);          
    String patternStr2 = "\\[\\[[\\w+\\s+]+"; 
    Pattern pattern2 = Pattern.compile(patternStr2);*/
    
		try{
      String lineOffset = key.toString();          
      //TODO eliminate the useless characters
      //Input String = "title    PageRank]outlink1]outlink2]...]"
      String inputKey[] = value.toString().split("\t");
      String inputValue[] = inputKey[1].split("]");
      double pageRank = Double.parseDouble(inputValue[0]);
      ArrayList<String> outLinkList = new ArrayList<String>();
      String outLinkTmp = new String();
      outLinkTmp = "";
      for(int i=1; i<inputValue.length; i++){
        if(!outLinkList.contains(inputValue[i])){   
          outLinkList.add(inputValue[i]);
          outLinkTmp = outLinkTmp + inputValue[i] + "]";
          word.set(inputValue[i]);
          double ratio = pageRank / (inputValue.length-1);
          outVal.set(ratio+"]"+"v");
          output.collect(word, outVal);
        }
      }      
      outVal.set(outLinkTmp);    
      word.set(inputKey[0]);       
      output.collect(word, outVal);
		}catch(Exception e){
			e.printStackTrace();
			System.out.printf("inputValue = %s\n",value.toString());
		}    
  }
}
