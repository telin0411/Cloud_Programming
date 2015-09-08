package hw2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class InvIndex {

	public static void main(String[] args) throws Exception {
	     JobConf conf = new JobConf(InvIndex.class);
	     conf.setJobName("HW1");		

	     conf.setMapOutputKeyClass(Text.class);
	     conf.setMapOutputValueClass(Text.class);	     
	     conf.setOutputKeyClass(Text.class);
	     conf.setOutputValueClass(Text.class);
	     
	     conf.setMapperClass(MyMapper4.class);
	     conf.setReducerClass(MyReducer4.class);
	     
	     conf.setInputFormat(TextInputFormat.class);
	     conf.setOutputFormat(TextOutputFormat.class);
	     
	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
	     FileOutputFormat.setOutputPath(conf, new Path(args[1]));
      conf.setNumReduceTasks(1);           
	     
	     JobClient.runJob(conf);
	}
}
