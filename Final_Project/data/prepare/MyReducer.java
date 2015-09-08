package fp;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import java.util.ArrayList;

public class MyReducer extends MapReduceBase
    implements Reducer<Text, Text, Text, Text> {
    Text outVal = new Text();

	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      
    String outStr = new String();
    String strTmp = new String();
    //ArrayList<String> content = new ArrayList<String>(); 
    outStr = "";   
    double rating=0.0;
    double hardness=0.0;
    int idCnt=0;
    while (values.hasNext()) {   
      Text value = (Text) values.next();
      strTmp = value.toString();
      String val[] = strTmp.split(";");
      rating += Double.parseDouble(val[1]);
      hardness += Double.parseDouble(val[2]);
      idCnt++;
      //outStr = outStr + strTmp + ",";
    }
    rating = rating / idCnt;
    hardness = hardness / idCnt;
    outStr = rating + ";" + hardness;
    outVal.set(outStr);
    output.collect(key, outVal);
	}
}
