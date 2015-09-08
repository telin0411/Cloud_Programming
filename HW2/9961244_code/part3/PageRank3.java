package hw2;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;
import java.lang.Math;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class PageRank3 {

	public static void main(String[] args) throws Exception {
	     JobConf conf = new JobConf(PageRank3.class);
	     conf.setJobName("HW2");
            
	     conf.setMapOutputKeyClass(Text.class);
	     conf.setMapOutputValueClass(Text.class);	     
	     conf.setOutputKeyClass(Text.class);
	     conf.setOutputValueClass(Text.class);
	     
	     conf.setMapperClass(MyMapper3.class);
	     conf.setReducerClass(MyReducer3.class);
	     conf.setOutputKeyComparatorClass(KeyComparator.class);           
	     
	     conf.setInputFormat(TextInputFormat.class);
	     conf.setOutputFormat(TextOutputFormat.class);
	     
	     FileInputFormat.setInputPaths(conf, new Path(args[0]));       
       //FileInputFormat.setInputPaths(conf, new Path("outTable/part-00000"));
	     FileOutputFormat.setOutputPath(conf, new Path(args[1]));   
       JobClient.runJob(conf);
	}
}
