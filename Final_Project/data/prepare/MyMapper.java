package fp;

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

public class MyMapper extends MapReduceBase
	implements Mapper<Object, Text, Text, Text> {

  private final IntWritable one = new IntWritable(1);
  private Text word = new Text();
  private Text outVal = new Text(); 
  
  @Override
  public void map(Object key, Text value,
      OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
          
		try{
      String lineOffset = key.toString();          
      String inputStr = value.toString();
      int i;
      String inputKey[] = inputStr.split("\t");
      String str[] = inputKey[1].split(",");
      for(i=0; i<str.length; i++){
        outVal.set(inputKey[0]+";"+str[i].substring(8,9)+";"+str[i].substring(7,8));
	      word.set(str[i].substring(0,7));
	      output.collect(word, outVal);
      }
            
		}catch(Exception e){
			e.printStackTrace();
			System.out.printf("inputValue = %s\n",value.toString());
		}    
  }
}
