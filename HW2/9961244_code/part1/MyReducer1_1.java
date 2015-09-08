package hw2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import java.util.ArrayList;

public class MyReducer1_1 extends MapReduceBase
    implements Reducer<Text, Text, Text, Text> {
    private Text outVal = new Text();
    private Text outVal1 = new Text();
    private Text word = new Text();    
    private Text word1 = new Text();    
    public static String titleStr = new String();  

	@Override
  public void configure(JobConf job){
    titleStr = job.get("titleList");
  }   
 
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

    String outStr = new String();
    String inputKey = new String();
    String nullStr = new String();    
    //ArrayList<String> content = new ArrayList<String>(); 
    outStr = "";  
    nullStr = ""; 
    inputKey = key.toString();
    int i;
    if(inputKey.equals("nullNode")){
      while (values.hasNext()) {   
        Text value = (Text) values.next();
        nullStr = "nullNode]";
        String outputKey = new String();
        outputKey = value.toString();
        //outStr = outStr + outputKey + "]";
        word1.set(outputKey);       
        outVal1.set("1]"+nullStr);
        output.collect(word1, outVal1);
      }
      outStr = titleStr;
    }
    else{
      Text value = (Text) values.next();
      String str[] = value.toString().split("]");
      for(i=1; i<str.length; i++){
        outStr = outStr + str[i] + "]";
      }
    }
    word.set(inputKey);
    outVal.set("1]"+outStr);
    output.collect(word, outVal);
	}
}
