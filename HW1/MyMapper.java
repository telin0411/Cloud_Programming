package hw1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;

public class MyMapper extends MapReduceBase
implements Mapper<Object, Text, OutputKey, OutputValue>{

	private OutputValue outputValue = new OutputValue(); 
	private OutputKey outputKey = new OutputKey();
  //private String fileName;
  //File
  private String fileName;
  public void configure(JobConf conf){
    String inputPath = new String(conf.get("map.input.file"));
		String inputPathTmp[] = inputPath.split("/");
    fileName = inputPathTmp[inputPathTmp.length-1];   
  }
  // Elimination function
  /*public void testString(String s){
    int tail;
    tail = s.length() - 1;
    char c1 = s.charAt(0);
    char c2 = s.charAt(tail);
    if((c1>='a' && c1<='z') || (c1>='A' && c1<='Z')){
      if((c1>='a' && c1<='z') || (c1>='A' && c1<='Z')){
       // ;
      }
      else{
        s = s.substring(0, tail-1);
      }
    }
    else{
      if((c2>='a' && c2<='z') || (c2>='A' && c2<='Z')){
        s = s.substring(1, tail);
      }
      else{
        s = s.substring(1, tail-1);
      }
    }
  }*/
      
	public void map(Object key, Text value, OutputCollector<OutputKey, OutputValue> output, Reporter reporter
					) throws IOException{	
		try{
			//output <key, value> = <word , (sum , cnt)>
			String inputKey[] = value.toString().split(" ");      
      
      //TODO eliminate the useless characters
      String str[] = inputKey[0].split("\\W");
			int i;
      for(i=0; i<str.length; i++){
			  //TODO transfer input data to <Text key, FloatWritable value> pair     
        int a = Integer.parseInt(inputKey[1]);		
    	  outputKey.set(str[i], fileName, 1);      
        outputValue.set(1, a);    	
        output.collect(outputKey, outputValue);  
      }      
			//output.collect(TEXT, COUNTVALUE);
		}catch(Exception e){
			e.printStackTrace();
			System.out.printf("inputValue = %s\n",value.toString());
		}
	}
}
