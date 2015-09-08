package hw1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;



public class MyReducer extends MapReduceBase
implements Reducer<OutputKey,OutputValue,OutputKey,OutputValue> {
		
	private FloatWritable result = new FloatWritable();
	private OutputValue outputValue = new OutputValue(); 
	private OutputKey outputKey = new OutputKey();
	
	public void reduce(OutputKey outputKey, Iterator<OutputValue> values,
			OutputCollector<OutputKey,OutputValue> output, Reporter reporter
			) throws IOException {

		//TODO count average
   
		float sum = 0;
		float c = 0;
    int temp_sum = 0;
    int temp_cnt = 0;
		while (values.hasNext()) {
      /*CountValue val = (CountValue)values.next();          
      temp_sum = val.getSum();
      temp_cnt = val.getCnt();
      sum += temp_sum;
      c += temp_cnt;*/  
		}
		//result.set(sum/c);
		output.collect(outputKey,outputValue);
	}

}
