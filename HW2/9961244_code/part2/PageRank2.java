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
import org.apache.hadoop.mapred.*;

public class PageRank2 {

	public static void main(String[] args) throws Exception {
       boolean status = false;
	     JobConf conf = new JobConf(PageRank2.class);
	     conf.setJobName("HW2");
       int titleNum = 0;
       // Read the input file and count the number of the titles
       Path inTable = new Path(args[0]);
       FileSystem fsIn = FileSystem.get(new Configuration());
       FSDataInputStream fstreamIn = fsIn.open(inTable);
       BufferedReader br = new BufferedReader(new InputStreamReader(fstreamIn)); 
       String tmp = new String();
       while(br.ready()){
         //tableTmp.add(br.readLine()); 
         tmp = br.readLine();
         titleNum++; 
       }
       //titleNum = 85;
       String titleStr = new String();
       titleStr = "Value=" + titleNum;
       conf.set("titleNum", titleStr);
       
	     conf.setMapOutputKeyClass(Text.class);
	     conf.setMapOutputValueClass(Text.class);	     
	     conf.setOutputKeyClass(Text.class);
	     conf.setOutputValueClass(Text.class);
	     
	     conf.setMapperClass(MyMapper2.class);
	     conf.setReducerClass(MyReducer2.class);
	     
	     conf.setInputFormat(TextInputFormat.class);
	     conf.setOutputFormat(TextOutputFormat.class);
	     
	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
       Path inputPaths[] = FileInputFormat.getInputPaths(conf);
       for (int j=0; j<inputPaths.length; j++) {
         System.out.println(inputPaths[j]);
       }
       int i=1;
       FileOutputFormat.setOutputPath(conf, new Path(args[1]+i));    
       JobClient.runJob(conf);
       
       // Iteration from 2 to limitation point
       JobConf[] conf1 = new JobConf[15]; //Declare the new JobConf array       
       for(i=0; i<9; i++){        
         //JobConf conf1 = new JobConf(PageRank2.class);
         //conf1[0] = JobConf(PageRank2.class);
         conf1[i] = new JobConf(PageRank2.class);
	       conf1[i].setJobName("HW2");
         conf1[i].set("titleNum", titleStr);       
	       conf1[i].setMapOutputKeyClass(Text.class);
	       conf1[i].setMapOutputValueClass(Text.class);	     
	       conf1[i].setOutputKeyClass(Text.class);
	       conf1[i].setOutputValueClass(Text.class);	     
	       conf1[i].setMapperClass(MyMapper2.class);
	       conf1[i].setReducerClass(MyReducer2.class);	     
	       conf1[i].setInputFormat(TextInputFormat.class);
	       conf1[i].setOutputFormat(TextOutputFormat.class);	     
	       FileInputFormat.setInputPaths(conf1[i], new Path(args[1]+(i+1)+"/part-00000"));
         FileOutputFormat.setOutputPath(conf1[i], new Path(args[1]+(i+2)));    
         JobClient.runJob(conf1[i]);                 
       }
       //     
	}
}
