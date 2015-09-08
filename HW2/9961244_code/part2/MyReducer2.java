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

public class MyReducer2 extends MapReduceBase
    implements Reducer<Text, Text, Text, Text> {
    Text outVal = new Text();
    Text word = new Text();
    public static int titleNum;
	@Override
  public void configure(JobConf job){
    String titleStr = new String();
    titleStr = job.get("titleNum");
    String str[] =  titleStr.split("=");
    titleNum = Integer.parseInt(str[1]);
  }
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		int sum = 0;
    double randomJump = 0.15;
    ArrayList<String> outLinkList = new ArrayList<String>();
    String outStr = new String();
    String title = new String();
    String firstLink = new String();
    title = key.toString();
    double pageRank = 0.0;
    outStr = "";   
    while (values.hasNext()) {   
      Text value = (Text) values.next();
      String str[] = value.toString().split("]");
      int judgePR = 0;
      if(str.length>1 && str[1].equals("v")){
        judgePR = 1;
      }
      else{
        judgePR = 0;
      }
      if(judgePR==0){
        if(str[0].equals("nullNode")){
          firstLink = "nullNode";
        }
        else{
          firstLink = str[0];
          for(int i=0; i<str.length; i++){
            if(!str[i].equals("")){
              outStr = outStr + str[i] + "]";
            }
          }
        }
      }
      else{
        double prTmp = Double.parseDouble(str[0]);
        pageRank += prTmp;
      }
      //
    }    
    if(firstLink.equals("nullNode")){
      pageRank = 1.0;
    }
    else{
      pageRank = randomJump / titleNum + ((1-randomJump) * pageRank);    
    }
    word.set(title);
    outVal.set(pageRank+"]"+outStr);
    output.collect(word, outVal);
	}
}
