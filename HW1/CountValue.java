package hw1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class CountValue implements Writable{
	private int sum;
	private int cnt;
	
	public CountValue(){
		this.sum =0;
		this.cnt = 0;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(sum);
		out.writeInt(cnt);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		sum = in.readInt();
		cnt = in.readInt();		
	}
	public String toString(){						
		return sum + cnt + "";
	}
	public void set(int sum, int cnt){
		this.sum = sum;
		this.cnt = cnt;
	}
	public int getSum(){
		return this.sum;
	}
	public int getCnt(){
		return this.cnt;
	}
}
