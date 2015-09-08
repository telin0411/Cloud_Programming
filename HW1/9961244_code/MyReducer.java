package hw1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import java.util.ArrayList;

public class MyReducer extends MapReduceBase
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
		while (values.hasNext()) {   
		  Text value = (Text) values.next();
		  String inputKey[] = value.toString().split(" ");     
      if(!fileName.contains(inputKey[0])){ 
        fileName.add(inputKey[0]); 
        sumTmp = String.valueOf(sum);
        if(LOTmp.length()>0){
          LOTmp = LOTmp.substring(0,LOTmp.length()-1);
        }
        TF.add(sumTmp+"["+LOTmp+"]");
        sum = 0;
        LOTmp = "";
      }
      //int a = Integer.parseInt(inputKey[1]);
      sum += 1;
      LOTmp = LOTmp + inputKey[2] + ",";
		}
    sumTmp = String.valueOf(sum);
    if(LOTmp.length()>0){
      LOTmp = LOTmp.substring(0,LOTmp.length()-1);
    }    
    TF.add(sumTmp+"["+LOTmp+"]");
    int df = fileName.size();
    int i;
    String outValTmp = new String();
    outValTmp = "";   
    for(i=0; i<df; i++){
      if(i<df-1){
        outValTmp = outValTmp + fileName.get(i) + " " + TF.get(i+1) + ";"; 
      }      
      else{
        outValTmp = outValTmp + fileName.get(i) + " " + TF.get(i+1);
      }
    }
    outVal.set(df+":"+outValTmp);
		output.collect(key, outVal);
	}
}
