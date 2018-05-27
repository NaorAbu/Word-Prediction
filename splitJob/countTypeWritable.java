package ass2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class countTypeWritable implements Writable{
	// Some data   
	private LongWritable c;
	private IntWritable type;

	public countTypeWritable(LongWritable c, IntWritable type) {
		this.c = c;
		this.type = type;
	}

	@Override
	public String toString() {
		return  c + " " + type;
	}

	public void write(DataOutput out) throws IOException {
		c.write(out);
		type.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		c.readFields(in);
		type.readFields(in);
	}
	
	public static pairWritable read(DataInput in) throws IOException {
		pairWritable w = new pairWritable();
		w.readFields(in);
		return w;
	}

	public LongWritable getC() {
		return c;
	}

	public void setC(LongWritable c) {
		this.c = c;
	}

	public IntWritable getType() {
		return type;
	}

	public void setType(IntWritable type) {
		this.type = type;
	}
	
	
	
}





