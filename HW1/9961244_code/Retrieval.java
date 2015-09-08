package hw1;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;
import java.lang.Math;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

public class Retrieval {

	public static void main(String[] args) throws Exception {
    
    // Array lists for query
    ArrayList<String> tableTmp = new ArrayList<String>();
    ArrayList<String> keyword = new ArrayList<String>();
    ArrayList<String> fileName = new ArrayList<String>();
    ArrayList<String> LO = new ArrayList<String>();     
    ArrayList<String> LOout = new ArrayList<String>(); 
    ArrayList<String> fileNameTable = new ArrayList<String>();  
    ArrayList<Integer> keywordNum = new ArrayList<Integer>();
    ArrayList<Integer> DF = new ArrayList<Integer>();
    ArrayList<Integer> TF = new ArrayList<Integer>();    
    ArrayList<Integer> andFlag = new ArrayList<Integer>();
    ArrayList<Integer> notFlag = new ArrayList<Integer>();    
    ArrayList<Double> Weight = new ArrayList<Double>(); 
    ArrayList<Double> wSort = new ArrayList<Double>();
    ArrayList<String> rankFile = new ArrayList<String>();    
    
    // Declrations
    int i = 0, j = 0, k = 0;
    double N = 44.0;
    String lnTmp = new String();
    
    // Read the output table file
    /*
    FileReader fr = new FileReader("outTable");
    BufferedReader br = new BufferedReader(fr);
    while(br.ready()){
      tableTmp.add(br.readLine());  
    }
    int fileNum = tableTmp.size();*/
    
    Path inTable = new Path(args[0]);
    FileSystem fsIn = FileSystem.get(new Configuration());
    FSDataInputStream fstreamIn = fsIn.open(inTable);
    BufferedReader br = new BufferedReader(new InputStreamReader(fstreamIn)); 
    while(br.ready()){
      tableTmp.add(br.readLine());  
    }
    int fileNum = tableTmp.size();       
    //System.out.println(lineTmp); 
    
    // Read in the keyword and store
    int judgeAnd = 0, judgeNot = 0; // judge if && appears in the query
    System.out.println("Please enter the keyword you'd like to query:");    
    Scanner sc = new Scanner(System.in);
    String inputData = sc.nextLine();
    System.out.println("You required to search for: "); 
    String keywordTmp[] = inputData.split(" ");
    
    // Record the useful keyword
    int kNum = 1;
    for(i=0; i<keywordTmp.length; i++){
      if(keywordTmp[i].equals("&&")){
        System.out.println("AND");   
        keyword.add(keywordTmp[i]);
      }
      else if(keywordTmp[i].equals("~")){
        System.out.println("NOT");   
        keyword.add(keywordTmp[i]);
      }
      else{
        System.out.println("Keyword "+kNum+" : "+keywordTmp[i]);  
        keyword.add(keywordTmp[i]);
        kNum++;
      }
    }    
    
    /* -1 == AND while -2 == NOT */
    // Search if the keyword could be found
    int kIndex = 0;
    for(i=0; i<fileNum; i++){
      String Table[] = tableTmp.get(i).split("\t"); 
      //System.out.println(Table[0]);
      fileNameTable.add(Table[0]);
    }
    for(i=0; i<keyword.size(); i++){
      if(fileNameTable.contains(keyword.get(i))){
        int fileNamePos = fileNameTable.indexOf(keyword.get(i));
        System.out.println("Matched case: "+keyword.get(i)+" at "+fileNamePos); 
        keywordNum.add(fileNamePos);
        kIndex = keyword.indexOf(keyword.get(i));
        if(kIndex != keyword.size()-1){
          String andNot = keyword.get(kIndex+1);
          if(andNot.equals("&&")){
            keywordNum.add(-1);
          }
          else if(andNot.equals("~")){
            keywordNum.add(-2);
          }
        }
      }
    }
    //System.out.println(keywordNum);   
    if(keywordNum.size()>0){
      for(i=0; i<keywordNum.size(); i++){
        // Judge of there is and / not character
        if(keywordNum.get(i)<0){
          i++;
        }
        if(i-1>=0){
          if(keywordNum.get(i-1)==-1){
            //System.out.println("AND!!");
            judgeAnd = 1;
          }
          else if(keywordNum.get(i-1)==-2){
            //System.out.println("NOT!!");  
            judgeNot = 1;
          }
        }
        String infoTmp1[] = tableTmp.get(keywordNum.get(i)).split("\t");  
        String infoTmp[] = infoTmp1[1].split(":");        
        int dfTmp = Integer.parseInt(infoTmp[0]);
        DF.add(dfTmp);
        //System.out.println("df = "+dfTmp);   
        String infoTmp2[] = infoTmp[1].split(";");
        for(j=0; j<infoTmp2.length; j++){
          //System.out.println(infoTmp2[i]);  
          String infoTmp3[] = infoTmp2[j].split(" ");
          //System.out.println("FileName = "+infoTmp3[0]);
          //System.out.println(infoTmp3[0]+" = "+infoTmp3[1]);
          String info[] = infoTmp3[1].split("\\D");
          //System.out.println("lineOffset = "+lnTmp); 
          int tfTmp = Integer.parseInt(info[0]);  
          // add to the arraylist and merge if in the same file
          double weight =  tfTmp * Math.log(N / dfTmp);        
          if(!fileName.contains(infoTmp3[0]) && judgeAnd==0 && judgeNot==0){
            //System.out.println("OR!!");
            fileName.add(infoTmp3[0]);
            lnTmp = "";
            for(k=1; k<info.length; k++){
            //System.out.println("lineOffset = "+info[k]);  
              lnTmp = lnTmp + info[k] + ",";
            }
            LO.add(lnTmp);
            //System.out.println(LO);            
            TF.add(tfTmp);
            Weight.add(weight);  
          }
          else if (fileName.contains(infoTmp3[0]) && judgeAnd==0 && judgeNot==0){
            //System.out.println("OR!!");
            String obj = infoTmp3[0];
            int retVal = fileName.indexOf(obj);
            //int tfTmp1 = TF.get(retVal);
            double weightTmp = Weight.get(retVal);
            String lnTmp1 = new String();
            lnTmp1 = LO.get(retVal);
            //System.out.println("retVal = "+retVal+" "+weight+" "+weightTmp);            
            weight += weightTmp;  
            lnTmp = "";
            for(k=1; k<info.length; k++){
            //System.out.println("lineOffset = "+info[k]);  
              lnTmp = lnTmp + info[k] + ",";
            }            
            lnTmp1 += lnTmp;
            Weight.remove(retVal);
            LO.remove(retVal);
            Weight.add(retVal, weight);
            LO.add(retVal, lnTmp1);
            TF.add(tfTmp);
          }         
          else if(fileName.contains(infoTmp3[0]) && judgeAnd==1 && judgeNot==0){
            //System.out.println("AND!!");
            String obj = infoTmp3[0];
            int retVal = fileName.indexOf(obj);
            double weightTmp = Weight.get(retVal);
            String lnTmp1 = new String();
            lnTmp1 = LO.get(retVal);            
            weight += weightTmp;  
            lnTmp = "=";
            for(k=1; k<info.length; k++){
            //System.out.println("lineOffset = "+info[k]);  
              lnTmp = lnTmp + info[k] + ",";
            }            
            lnTmp1 += lnTmp;
            Weight.remove(retVal);
            LO.remove(retVal);
            Weight.add(retVal, weight);
            LO.add(retVal, lnTmp1);
            //System.out.println(LO);
            TF.add(tfTmp);  
            andFlag.add(retVal);          
          }       
          else if(fileName.contains(infoTmp3[0]) && judgeAnd==0 && judgeNot==1){
            //System.out.println("AND!!");
            String obj = infoTmp3[0];
            int retVal = fileName.indexOf(obj);
            Weight.remove(retVal);
            LO.remove(retVal);
            fileName.remove(retVal);      
          }          
        }         
        //
        //System.out.println("andFlag = "+andFlag);
        if(judgeAnd==1 && judgeNot==0){  
          ArrayList<String> andfileName = new ArrayList<String>();
          ArrayList<String> andLO = new ArrayList<String>();    
          ArrayList<Double> andWeight = new ArrayList<Double>();           
          for(k=0; k<andFlag.size(); k++){ 
            andfileName.add(fileName.get(andFlag.get(k)));
            andWeight.add(Weight.get(andFlag.get(k)));
            andLO.add(LO.get(andFlag.get(k)));
          }
          fileName = andfileName;
          Weight = andWeight;
          LO = andLO;
          andFlag.clear();       
          judgeAnd = 0;
          judgeNot = 0;        
          //System.out.println("Now we have "+fileName.size()+" files");
        }
        else if(judgeAnd==0 && judgeNot==1){        
          judgeAnd = 0;
          judgeNot = 0;        
        }        
        //
      }
      //System.out.println("Final fileName = "+fileName);  
      //System.out.println("Final weight = "+Weight); 
      int fnNum = fileName.size();
      int tfNum = TF.size();
      int wNum = Weight.size();
      //int wsNum = wSort.size();
      int wsNum = 1;
      int judgews = 0;
      
      // Judge if the result list is empty
      if(fnNum>0){
        double w = Weight.get(0);
        wSort.add(w);      
        // Sorting
        for(i=1; i<wNum; i++){
          w = Weight.get(i);
          for(j=0; j<wsNum; j++){          
            if(w >= wSort.get(j)){
              //System.out.println(w+" "+wSort.get(j));
              wSort.add(j, w);
              judgews = 1;     
              break;       
            }
          }
          if(judgews == 1){
            judgews = 0;
            wsNum = wSort.size(); 
            //System.out.println("size = "+wsNum);
          }
          else{
            wSort.add(w);
            wsNum = wSort.size();
          }
          //System.out.println(wSort);        
        }
        //System.out.println(wSort);
        int rankNum; 
        if(wSort.size() < 10){
          rankNum = wSort.size();
        }
        else{
          rankNum = 10;
        }
        System.out.println("\n******* Query Result *******");
        for(i=0; i<rankNum; i++){
          double qweight = wSort.get(i);
          int retIndex = Weight.indexOf(qweight);
          System.out.print("Rank"+(i+1)+" : "+fileName.get(retIndex)+" score=");
          System.out.printf("%.3f\n", qweight);
          //System.out.println("Line offset = "+LO.get(retIndex));
          rankFile.add(fileName.get(retIndex));
          Weight.remove(retIndex);
          //System.out.println(Weight);
          fileName.remove(retIndex);
          //System.out.println(fileName);
          LOout.add(LO.get(retIndex));          
          LO.remove(retIndex);
          //System.out.println(LOout);
          // Open the required file to extract the fragment
          String lineTmp = new String();
          String lineTmp1 = new String();
          String[] diffOffset = LOout.get(i).split("=");
          // Extract the input file via the input stream
          //Path fileToRead = new Path("/opt/HW1/input1/"+rankFile.get(i));
          Path fileToRead = new Path(args[1]+rankFile.get(i));
          FileSystem fs = FileSystem.get(new Configuration());
          FSDataInputStream fstream = fs.open(fileToRead);
          int diffNum = diffOffset.length;
          for(k=0; k<diffNum; k++){
            String[] lnOffset = diffOffset[k].split("\\D");
            System.out.print("Fragment "+(k+1)+": ");
            int loNum = lnOffset.length;
            int[] lineOffset = new int[loNum];
            for(j=0; j<loNum; j++){
              lineOffset[j] = Integer.parseInt(lnOffset[j]);
              //System.out.println("lineOffset = "+lineOffset[j]);
            }
            fstream.seek(lineOffset[0]); 
            BufferedReader br1 = new BufferedReader(new InputStreamReader(fstream)); 
            lineTmp = br1.readLine();        
            System.out.println(lineTmp);
          }
          System.out.println("---------------------------------------------");          
        }        
        //
      }
      else{
        System.out.println("******* Query Result *******");    
        System.out.println("Sorry, the query could not be found!!");
      }               
    }
    else{
      System.out.println("******* Query Result *******");    
      System.out.println("Sorry, the query could not be found!!");
    }
    
	}
}
