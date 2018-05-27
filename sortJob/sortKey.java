package ass2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class sortKey implements WritableComparable<sortKey> {

	Text w;
	DoubleWritable c;

	public sortKey() {
		this.w = new Text();
		this.c = new DoubleWritable();
	}
	
	public sortKey(Text w, DoubleWritable c) {
		String[] split = w.toString().split("\\s+");
		String words = split[0] + " " + split[1];
		this.w = new Text(words);
		this.c = c;
	}

	public void readFields(DataInput in) throws IOException {
		w.readFields(in);
		c.readFields(in);

	}
	
	public String toString() {
		return w.toString() + " " + c.toString();
	}

	public void write(DataOutput out) throws IOException {
		w.write(out);
		c.write(out);
	}
	
	public static sortKey read(DataInput in) throws IOException {
		sortKey w = new sortKey();
		w.readFields(in);
		return w;
	}

	public int compareTo(sortKey o) {
		int ans = this.w.compareTo(o.getW());
		if (ans == 0) ans = -1*c.compareTo(o.getC());
		return ans;
	}
	

	public Text getW() {
		return w;
	}

	public void setW(Text w) {
		this.w = w;
	}

	public DoubleWritable getC() {
		return c;
	}

	public void setC(DoubleWritable c) {
		this.c = c;
	}

}
