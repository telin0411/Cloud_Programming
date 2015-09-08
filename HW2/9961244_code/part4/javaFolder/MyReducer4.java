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

public class MyReducer4 extends MapReduceBase
    implements Reducer<Text, Text, Text, Text> {
    Text outVal = new Text();

	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		int sum = 0;
    ArrayList<String> fileName = new ArrayList<String>();
    ArrayList<String> TF = new ArrayList<String>();
    String sumTmp = new String();  
    String LOTmp = new String();
    String inputKey = new String();  
    while (values.hasNext()) {   
      Text value = (Text) values.next();
      inputKey = value.toString();     
      if(!fileName.contains(inputKey)){ 
        fileName.add(inputKey); 
        sumTmp = String.valueOf(sum);
        TF.add(sumTmp);
        sum = 0;
      }
      sum += 1;
		}
    sumTmp = String.valueOf(sum);
    TF.add(sumTmp);
    int df = fileName.size();
    int i;
    String outValTmp = new String();
    outValTmp = "";   
    for(i=0; i<df; i++){
      if(i<df-1){
        outValTmp = outValTmp + fileName.get(i) + "===" + TF.get(i+1) + ";;;"; 
      }      
      else{
        outValTmp = outValTmp + fileName.get(i) + "===" + TF.get(i+1);
      }
    }
    outVal.set(df+":::"+outValTmp);
		output.collect(key, outVal);
	}
}
