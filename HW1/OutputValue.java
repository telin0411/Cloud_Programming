package hw1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class OutputValue implements Writable{
	private int df;
	private int tf;
	
	public OutputValue(){
		this.df =0;
		this.tf = 0;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(df);
		out.writeInt(tf);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		df = in.readInt();
		tf = in.readInt();		
	}
	public String toString(){						
		return df + " " + tf + "";
	}
	public void set(int df, int tf){
		this.df = df;
		this.tf = tf;
	}
	public int getDf(){
		return this.df;
	}
	public int getTf(){
		return this.tf;
	}
}
