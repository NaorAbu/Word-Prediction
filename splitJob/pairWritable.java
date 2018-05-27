package ass2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class pairWritable implements Writable{
	// Some data   
	private Text w;
	private LongWritable c;
	private IntWritable type;
	public pairWritable() {
		this.w = new Text();
		this.c = new LongWritable();
		this.type = new IntWritable();
	}

	public pairWritable(Text w,IntWritable type) {
		this.w = new Text(w.toString());
		this.c = new LongWritable(0);
		this.type = new IntWritable(type.get());
	}
	
	public pairWritable(Text w,Text c) {
		this.w = new Text(w.toString());
		this.c = new LongWritable(Long.parseLong(c.toString()));
		type = new IntWritable(6);
	}

	public void write(DataOutput out) throws IOException {
		w.write(out);
		c.write(out);
		type.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		w.readFields(in);
		c.readFields(in);
		type.readFields(in);
	}
	
	public static pairWritable read(DataInput in) throws IOException {
		pairWritable w = new pairWritable();
		w.readFields(in);
		return w;
	}
	
	public Text getW() {
		return w;
	}

	public LongWritable getC() {
		return c;
	}
	public IntWritable getType() {
		return type;
	}

	public void setW(Text w) {
		this.w = w;
	}

	public void setC(LongWritable c) {
		this.c = c;
	}

	public void setType(IntWritable type) {
		this.type = type;
	}

	@Override
	public String toString() {
		return w + " " + c + " " + type ;
	}	
}





