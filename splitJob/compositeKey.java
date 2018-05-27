package ass2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class compositeKey implements WritableComparable<compositeKey>{
	// Some data   
	private Text w;
	private BooleanWritable isCounter;
	
	
	public compositeKey() {
		this.w = new Text();
		this.isCounter = new BooleanWritable();
	}

	public compositeKey(Text w,boolean isCounter) {
		this.w = new Text(w.toString());
		this.isCounter = new BooleanWritable(isCounter);
	}

	public void write(DataOutput out) throws IOException {
		w.write(out);
		isCounter.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		w.readFields(in);
		isCounter.readFields(in);
	}
	
	public static compositeKey read(DataInput in) throws IOException {
		compositeKey w = new compositeKey();
		w.readFields(in);
		return w;
	}

	public Text getW() {
		return w;
	}

	public void setW(Text w) {
		this.w = w;
	}

	public BooleanWritable getIsCounter() {
		return isCounter;
	}

	public void setIsCounter(BooleanWritable isCounter) {
		this.isCounter = isCounter;
	}

	@Override
	public String toString() {
		return w + " " + isCounter ;
	}

	public int compareTo(compositeKey other) {
		// TODO Auto-generated method stub
		
		int result = this.w.compareTo(other.getW());
		if(result==0) {
			if (isCounter.get()) result = -1;
			else result =1;
		}
		return result;
	}	
}





