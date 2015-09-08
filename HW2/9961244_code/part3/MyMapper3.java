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

public class MyMapper3 extends MapReduceBase
	implements Mapper<Object, Text, Text, Text> {

  private final IntWritable one = new IntWritable(1);
  private Text word = new Text();
  private Text outVal = new Text(); 
  
  @Override
  public void map(Object key, Text value,
      OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
   
		try{
      String lineOffset = key.toString();          
      //TODO eliminate the useless characters
      //Input String = "title    PageRank]outlink1]outlink2]...]"
      String inputKey[] = value.toString().split("\t");
      String inputValue[] = inputKey[1].split("]");
      double pageRank = Double.parseDouble(inputValue[0]);
      String PR = new String();
      PR = inputValue[0];
      word.set(PR); 
      outVal.set(inputKey[0]);  
      output.collect(word, outVal);
		}catch(Exception e){
			e.printStackTrace();
			System.out.printf("inputValue = %s\n",value.toString());
		}    
  }
}
