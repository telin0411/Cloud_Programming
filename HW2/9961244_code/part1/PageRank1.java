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
import java.util.ArrayList;

public class PageRank1 {

	public static void main(String[] args) throws Exception {
	     JobConf conf = new JobConf(PageRank1.class);
	     conf.setJobName("HW2");
       
	     conf.setMapOutputKeyClass(Text.class);
	     conf.setMapOutputValueClass(Text.class);	     
	     conf.setOutputKeyClass(Text.class);
	     conf.setOutputValueClass(Text.class);
	     
	     conf.setMapperClass(MyMapper1.class);
	     conf.setReducerClass(MyReducer1.class);
	     
	     conf.setInputFormat(TextInputFormat.class);
	     conf.setOutputFormat(TextOutputFormat.class);
	     
	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
	     FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	     
	     JobClient.runJob(conf);
       
	     JobConf conf1 = new JobConf(PageRank1.class);
	     conf1.setJobName("HW2");
           
       // Read the input file and count the number of the titles
       Path inTable = new Path(args[1]+"/part-00000");
       FileSystem fsIn = FileSystem.get(new Configuration());
       FSDataInputStream fstreamIn = fsIn.open(inTable);
       BufferedReader br = new BufferedReader(new InputStreamReader(fstreamIn)); 
       String tmp = new String();
       String titleList = new String();
       String nodeList = new String();
       titleList = "";
       nodeList = "";
       int i;
       while(br.ready()){
         //tableTmp.add(br.readLine()); 
         tmp = br.readLine();
         String str1[] = tmp.split("\t");
         titleList = titleList + str1[0] + "]";
         String str2[] = str1[1].split("]");
         for(i=1; i<str2.length; i++){
           nodeList = nodeList + str2[i] + "]";
         }
       }          
       //System.out.println(nodeList);  
       //System.out.println(titleList); 
       conf1.set("titleList", titleList);
       conf1.set("nodeList", nodeList);
       
	     conf1.setMapOutputKeyClass(Text.class);
	     conf1.setMapOutputValueClass(Text.class);	     
	     conf1.setOutputKeyClass(Text.class);
	     conf1.setOutputValueClass(Text.class);
	     
	     conf1.setMapperClass(MyMapper1_1.class);
	     conf1.setReducerClass(MyReducer1_1.class);
	     
	     conf1.setInputFormat(TextInputFormat.class);
	     conf1.setOutputFormat(TextOutputFormat.class);
	     
	     FileInputFormat.setInputPaths(conf1, new Path(args[1]+"/part-00000"));
	     FileOutputFormat.setOutputPath(conf1, new Path(args[1]+"_v2"));
	     
	     JobClient.runJob(conf1);
	}
}
