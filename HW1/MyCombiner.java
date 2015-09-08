package hw1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class MyCombiner extends MapReduceBase
implements Reducer<OutputKey,OutputValue,OutputKey,OutputValue> {

	private OutputValue outputValue = new OutputValue();
	private OutputKey outputKey = new OutputKey();
  	
	public void reduce(OutputKey outputKey, Iterator<OutputValue> values,
			OutputCollector<OutputKey,OutputValue> output, Reporter reporter
			) throws IOException {	
		//TODO combine many CountValue to 1 CountValue  (work like reducer)
    int tmp_tf = 0;
    int tmp_df = 1;
		while(values.hasNext()){
			OutputValue val = (OutputValue)values.next();
      tmp_tf += val.getTf();
      //temp_cnt += val.getCnt();
      outputValue.set(tmp_df, tmp_tf);
		}		
		output.collect(outputKey,outputValue);
	}
}

