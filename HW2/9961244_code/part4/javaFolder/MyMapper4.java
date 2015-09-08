package hw2;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;

public class MyMapper4 extends MapReduceBase
	implements Mapper<Object, Text, Text, Text> {

  private final IntWritable one = new IntWritable(1);
  private Text word = new Text();
  private Text outVal = new Text();
  private String fileName;
  
  @Override
  public void map(Object key, Text value,
      OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

    /*String line = value.toString();
    StringTokenizer itr = new StringTokenizer(line);
    while(itr.hasMoreTokens()) {
      word.set(itr.nextToken());
      output.collect(word, one);
    }*/
    
    String patternStr = "<title>[^<]+";
    Pattern pattern = Pattern.compile(patternStr);
    int i;    
		try{
      String lineOffset = key.toString();  
      String inputStr = value.toString();
      Matcher matcher = pattern.matcher(inputStr);
      String term = new String();
      boolean matchFound = matcher.find();
      while(matchFound) {
        for(i=0; i<=matcher.groupCount(); i++) {
          String groupStr = matcher.group(i);
          //word.set(groupStr.substring(7,groupStr.length()));
          term = groupStr.substring(7,groupStr.length());
        }
        if(matcher.end() + 1 <= inputStr.length()) {
          matchFound = matcher.find(matcher.end());
        }
        else{
          break;
        }
      }      
      
      //TODO eliminate the useless characters
      String str1[] = inputStr.split("<text xml:space=");
      String str2[] = str1[1].split("</text>");
      String inputStr2 = str2[0];      
      String str[] = inputStr2.split("\\W");
      for(i=1; i<str.length; i++){
        if(str[i].compareTo("") != 0){
          outVal.set(term);
          word.set(str[i]);      	
          output.collect(word, outVal);
        }  
      }     
			//output.collect(TEXT, COUNTVALUE);
		}catch(Exception e){
			e.printStackTrace();
			System.out.printf("inputValue = %s\n",value.toString());
		}    
  }
}
