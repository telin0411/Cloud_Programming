package hw1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;


public class Wordaverage {

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Wordaverage.class);
		conf.setJobName("hw1");		
						
		conf.setMapperClass(MyMapper.class);
		conf.setCombinerClass(MyCombiner.class);
		conf.setReducerClass(MyReducer.class);

		conf.setOutputKeyClass(OutputKey.class);
		conf.setOutputValueClass(OutputValue.class);    

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));		
		
		conf.setNumMapTasks(3);
		conf.setNumReduceTasks(1);

		JobClient.runJob(conf);
	}
}
