package fp;

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

public class CompSim {

	public static void main(String[] args) throws Exception {
       boolean status = false;
	     JobConf conf = new JobConf(CompSim.class);
	     conf.setJobName("CompareSimilarity");
       // Read the input file of the student  
       BufferedReader br = new BufferedReader(new FileReader(args[2]));       
       String inputData = new String();
       while(br.ready()){
         inputData = br.readLine();
       }
       br.close(); 
       System.out.println(inputData);
              
       String inputStr = new String();
       String str[] = inputData.split("\t");
       inputStr = str[1];
       conf.set("inputStudent", inputStr);
       String courses[] = inputStr.split(",");
       int size1 = courses.length-1;
       int qSem; // the semester to be queried 
       qSem = Integer.parseInt(courses[size1].substring(courses[size1].length()-1));
       qSem++;
       
	     conf.setMapOutputKeyClass(Text.class);
	     conf.setMapOutputValueClass(Text.class);	     
	     conf.setOutputKeyClass(Text.class);
	     conf.setOutputValueClass(Text.class);
	     
	     conf.setMapperClass(CompMapper.class);
	     conf.setReducerClass(CompReducer.class);
	     conf.setOutputKeyComparatorClass(KeyComparator.class);            
	     
	     conf.setInputFormat(TextInputFormat.class);
	     conf.setOutputFormat(TextOutputFormat.class);
	     
	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
       int i=1;
       FileOutputFormat.setOutputPath(conf, new Path(args[1]));    
       JobClient.runJob(conf);
       
       int counter = 0;
       Path inTable = new Path(args[1]+"/part-00000");
       FileSystem fsIn = FileSystem.get(new Configuration());
       FSDataInputStream fstreamIn = fsIn.open(inTable);
       BufferedReader br1 = new BufferedReader(new InputStreamReader(fstreamIn)); 
       String tmp = new String();
       int j;
       // Rearrange the output format
       ArrayList<String> rankID = new ArrayList<String>();
       int limit;
       limit = 25;
       while(br1.ready() && counter<limit){
         tmp = br1.readLine();
         String str1[] = tmp.split("\t");
         String str2[] = str1[1].split(",");
         int size = str2.length;
         if(limit-counter>size){
           for(j=0; j<size; j++){
             String str3[] = str2[j].split("<");
             rankID.add(str3[0]);
             counter++;              
           }
         }
         else{
           size = limit - counter;
           for(j=0; j<size; j++){
             String str3[] = str2[j].split("<");
             rankID.add(str3[0]);
             counter++;              
           }           
         }
       }         
       System.out.println(rankID);
       
       // Store the result to the file
       Configuration config = new Configuration();
       FileSystem fs = FileSystem.get(config);
       Path outFile = new Path(args[3]);
       FSDataOutputStream out = fs.create(outFile);
       BufferedWriter br2 = new BufferedWriter(new OutputStreamWriter(out)); 
       String stu_IDs = new String();
       br2.write(qSem+";");
       for(j=0; j<limit; j++){
         stu_IDs = rankID.get(j);
         br2.write(stu_IDs+",");
       }
       stu_IDs = rankID.get(9);
       System.out.println("The query semester is "+qSem);
       br2.write(stu_IDs+"\n");
       br2.close();
       
	}
}
