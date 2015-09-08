package part0;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class MyMapper extends MapReduceBase
	implements Mapper<Object, Text, Text, IntWritable> {

  private final IntWritable one = new IntWritable(1);
  private Text word = new Text();
  
  @Override
  public void map(Object key, Text value,
      OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

    String line = value.toString();
    StringTokenizer itr = new StringTokenizer(line);
    while(itr.hasMoreTokens()) {
      word.set(itr.nextToken());
      output.collect(word, one);
    }
  }
}
