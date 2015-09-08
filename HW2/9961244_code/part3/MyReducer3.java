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

public class MyReducer3 extends MapReduceBase
    implements Reducer<Text, Text, Text, Text> {
    Text outVal = new Text();
    Text word = new Text();
    public static int titleNum;
	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    ArrayList<String> outLinkList = new ArrayList<String>();
    String strTmp = new String();
    String valueTmp = new String();
    strTmp = "";
    while(values.hasNext()){
      Text value = (Text) values.next();
      valueTmp = value.toString();
      strTmp = strTmp + valueTmp + "]";
      //word.set(title);
    }
    outVal.set(strTmp);
    //outVal.set(pageRank+"]"+outStr);
    output.collect(key, outVal);
	}
}
