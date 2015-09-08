package hw1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class OutputKey implements Writable{
	private String word;
	private String fileName;
  private int lineOffset;
	
	public OutputKey(){
		this.word = "";
		this.fileName = "";
    this.lineOffset = 0;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeChars(word);
		out.writeChars(fileName);
    out.writeInt(lineOffset);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		word = in.readLine();
		fileName = in.readLine();		
    lineOffset = in.readInt();
	}
	public String toString(){						
		return word + " " + fileName + " " + lineOffset + "";
	}
	public void set(String word, String fileName, int lineOffset){
		this.word = word;
		this.fileName = fileName;
    this.lineOffset = lineOffset;
	}
	public String getWord(){
		return this.word;
	}
	public String getFileName(){
		return this.fileName;
	}
	public int getLineOffset(){
		return this.lineOffset;
	} 
}
