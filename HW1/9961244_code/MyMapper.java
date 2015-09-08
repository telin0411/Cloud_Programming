package hw1;

import java.io.IOException;
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
  private String fileName;
  public void configure(JobConf conf){
    String inputPath = new String(conf.get("map.input.file"));
		String inputPathTmp[] = inputPath.split("/");
    fileName = inputPathTmp[inputPathTmp.length-1];   
  }  
  
  @Override
  public void map(Object key, Text value,
      OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

    /*String line = value.toString();
    StringTokenizer itr = new StringTokenizer(line);
    while(itr.hasMoreTokens()) {
      word.set(itr.nextToken());
      output.collect(word, one);
    }*/
		try{
			//output <key, value> = <word , (sum , cnt)>
			//String inputKey[] = value.toString().split(" ");    
      String lineOffset = key.toString();  
      
      //TODO eliminate the useless characters
      //String str[] = inputKey[0].split("\\W");
      String str[] = value.toString().split("\\W");
			int i;
      for(i=0; i<str.length; i++){
			  //TODO transfer input data to <Text key, FloatWritable value> pair   
        if(str[i].compareTo("") != 0){		
    	    word.set(str[i]);  
          outVal.set(fileName+" "+"1"+" "+lineOffset);      	
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
