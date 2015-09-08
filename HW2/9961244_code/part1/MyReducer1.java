package hw2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import java.util.ArrayList;

public class MyReducer1 extends MapReduceBase
    implements Reducer<Text, Text, Text, Text> {
    Text outVal = new Text();

	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      
    String outStr = new String();
    ArrayList<String> content = new ArrayList<String>(); 
    outStr = "";   
    //while (values.hasNext()) {   
    Text value = (Text) values.next();
    String str[] = value.toString().split("]");
    int i;
    for(i=1; i<str.length; i++){
      if(!content.contains(str[i])){   
        content.add(str[i]);
        outStr = outStr + str[i] + "]";
      }
    }
    //}
    outVal.set("1]"+outStr);
    output.collect(key, outVal);
	}
}
