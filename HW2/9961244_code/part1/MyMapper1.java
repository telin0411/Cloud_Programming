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

public class MyMapper1 extends MapReduceBase
	implements Mapper<Object, Text, Text, Text> {

  private final IntWritable one = new IntWritable(1);
  private Text word = new Text();
  private Text outVal = new Text(); 
  
  @Override
  public void map(Object key, Text value,
      OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      
    //String patternStr = "<title>[\\w+\\s+/+]+";
    String patternStr = "<title>[^<]+";
    Pattern pattern = Pattern.compile(patternStr);          
    String patternStr2 = "\\[\\[[^\\]]+\\]\\]"; 
    //String patternStr2 = "\\[\\[[\\w+\\s+/+]+"; 
    Pattern pattern2 = Pattern.compile(patternStr2);
    
		try{
      String lineOffset = key.toString();          
      //TODO eliminate the useless characters
      String inputStr = value.toString();
      Matcher matcher = pattern.matcher(inputStr);  
      boolean matchFound = matcher.find();
			int i;
      while(matchFound) {
        for(i=0; i<=matcher.groupCount(); i++) {
          String groupStr = matcher.group(i);
          word.set(groupStr.substring(7,groupStr.length()));
          //output.collect(word, outVal);
        }
        if(matcher.end() + 1 <= inputStr.length()) {
          matchFound = matcher.find(matcher.end());
        }
        else{
          break;
        }
      }
      String str[] = inputStr.split("<text");
      String str2[] = str[1].split("</text>");
      String inputStr2 = str2[0];
      Matcher matcher2 = pattern2.matcher(inputStr2);
      boolean matchFound2 = matcher2.find();
      String outValTmp = new String();
      outValTmp = "";  
      while(matchFound2) {
        for(i=0; i<=matcher2.groupCount(); i++) {
          String groupStr2 = matcher2.group(i);
          outValTmp = outValTmp + groupStr2.substring(2,groupStr2.length()-2) + "]";
        }
        if(matcher2.end() + 1 <= inputStr2.length()) {
          matchFound2 = matcher2.find(matcher2.end());
        }
        else{
          break;
        }
      }      
      outVal.set("1]"+outValTmp);           
      output.collect(word, outVal);
		}catch(Exception e){
			e.printStackTrace();
			System.out.printf("inputValue = %s\n",value.toString());
		}    
  }
}
