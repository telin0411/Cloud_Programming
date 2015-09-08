package fp;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import java.util.ArrayList;

class ValueComparator implements Comparator<String>{
    Map<String, Double> base;
    public ValueComparator(Map<String, Double> base) {
      this.base = base;
    }
    // Note: this comparator imposes orderings that are inconsistent with equals.    
    public int compare(String a, String b){
      if(base.get(a) >= base.get(b)) {
        return -1;
      } 
      else{
        return 1;
      } // returning 0 would merge keys
    }
}
public class CompReducer extends MapReduceBase
    implements Reducer<Text, Text, Text, Text> {
    Text outVal = new Text();
    Text word = new Text();
    public static int titleNum;
	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      
    HashMap<String, Double> stu = new HashMap<String, Double>();  
    HashMap<Integer, String> stuTmp = new HashMap<Integer, String>(); 
    HashMap<Integer, Integer> stuIndex = new HashMap<Integer, Integer>(); 
    ValueComparator vc =  new ValueComparator(stu);
    TreeMap<String,Double> sorted_stu = new TreeMap<String,Double>(vc);    
    
    String outStr = new String();
    String ID = new String();
    String tmp = new String();
    ID = key.toString();
    outStr = "";   
    int num=0;
    while (values.hasNext()) {   
      Text value = (Text) values.next();
      tmp = value.toString();
      String str1[] = tmp.split("<<");
      String str2[] = str1[1].split(">>");
      double rankSem = Double.parseDouble(str2[0]);
      stu.put(str1[0], rankSem);
      stuTmp.put(num, str1[0]);
      num++;
      //outStr = outStr + tmp + ",";
    }
    sorted_stu.putAll(stu); // sorting with repect to the rank
    int i;
    double rank; // The accumulated ranking score with repect to each semester
    String stuID = new String();
    // Store the sorted map
    Iterator it = sorted_stu.entrySet().iterator();
    while(it.hasNext()){
      Map.Entry pairs = (Map.Entry)it.next();
      outStr = outStr + pairs.getKey() + "<" + pairs.getValue() + ">" + ",";
    } 
    word.set(ID);
    outVal.set(outStr);
    output.collect(word, outVal);
    // Clear the hashmaps
    sorted_stu.clear();
    stu.clear();
    stuTmp.clear();
	}
}
