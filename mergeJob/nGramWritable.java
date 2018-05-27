package ass2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class nGramWritable implements Writable{
	// Some data   
	private LongWritable N1;//#w3
	private LongWritable N2;//#w2,w3
	private LongWritable N3;//#w1,w2,w3
	private LongWritable C1;//#w2
	private LongWritable C2;//#w1,w2

	
	public nGramWritable(LongWritable count, IntWritable type) {
		
		switch (type.get()) {
		case 1://C1
			N1 = new LongWritable(0);
			N2 = new LongWritable(0);
			N3 = new LongWritable(0);
			C1 = new LongWritable(count.get());
			C2 = new LongWritable(0);
			break;
		case 2://N1
			N1 = new LongWritable(count.get());
			N2 = new LongWritable(0);
			N3 = new LongWritable(0);
			C1 = new LongWritable(0);
			C2 = new LongWritable(0);
			break;
		case 3://C2
			N1 = new LongWritable(0);
			N2 = new LongWritable(0);
			N3 = new LongWritable(0);
			C1 = new LongWritable(0);
			C2 = new LongWritable(count.get());
			break;
		case 4://N2
			N1 = new LongWritable(0);
			N2 = new LongWritable(count.get());
			N3 = new LongWritable(0);
			C1 = new LongWritable(0);
			C2 = new LongWritable(0);
			break;
		case 5://N3
			N1 = new LongWritable(0);
			N2 = new LongWritable(0);
			N3 = new LongWritable(count.get());
			C1 = new LongWritable(0);
			C2 = new LongWritable(0);
		}
		
	}
	public nGramWritable(LongWritable c1, LongWritable n1, LongWritable c2, LongWritable n2, LongWritable n3) {
		N1 = new LongWritable(n1.get());
		N2 = new LongWritable(n2.get());
		N3 = new LongWritable(n3.get());
		C1 = new LongWritable(c1.get());
		C2 = new LongWritable(c2.get());
	}

	public nGramWritable() {
		this.N1 = new LongWritable(0);
		this.N2 = new LongWritable(0);
		this.N3 = new LongWritable(0);
		this.C1 = new LongWritable(0);
		this.C2 = new LongWritable(0);
	}


	public void write(DataOutput out) throws IOException {
		C1.write(out);
		N1.write(out);
		C2.write(out);
		N2.write(out);
		N3.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		C1.readFields(in);
		N1.readFields(in);
		C2.readFields(in);
		N2.readFields(in);
		N3.readFields(in);
	}

	public void merge(nGramWritable other) {
		this.N1.set(this.N1.get()+other.getN1().get());
		this.N2.set(this.N2.get()+other.getN2().get());
		this.N3.set(this.N3.get()+other.getN3().get());
		this.C1.set(this.C1.get()+other.getC1().get());
		this.C2.set(this.C2.get()+other.getC2().get());
	}

	public static nGramWritable read(DataInput in) throws IOException {
		nGramWritable w = new nGramWritable();
		w.readFields(in);
		return w;
	}
	
	public Double prob(String C0) {
		Long c0 = new Long(C0);
		Long n1 = new Long(this.getN1().get());
		Long n2 = new Long(this.getN2().get());
		Long n3 = new Long(this.getN3().get());
		Long c1 = new Long(this.getC1().get());
		Long c2 = new Long(this.getC2().get());
		Double k2 = new Double((Math.log10(n2.doubleValue()+1)+1)/(Math.log10(n2.doubleValue()+1)+2));
		Double k3 = new Double((Math.log10(n3.doubleValue()+1)+1)/(Math.log10(n3.doubleValue()+1)+2));
		Double p = new Double((k3*(n3.doubleValue()/c2.doubleValue()))
				+ ((1-k3)*k2*(n2.doubleValue()/c1.doubleValue()))
				+ (1-k3)*(1-k2)*(n1.doubleValue()/c0.doubleValue()));
		return p;
	}
	
	@Override
	public String toString() {
		return N1 + " " + N2 + " " + N3 + " " + C1 + " " + C2;
	}
	public LongWritable getN1() {
		return N1;
	}
	public void setN1(LongWritable n1) {
		N1 = n1;
	}
	public LongWritable getN2() {
		return N2;
	}
	public void setN2(LongWritable n2) {
		N2 = n2;
	}
	public LongWritable getN3() {
		return N3;
	}
	public void setN3(LongWritable n3) {
		N3 = n3;
	}
	public LongWritable getC1() {
		return C1;
	}
	public void setC1(LongWritable c1) {
		C1 = c1;
	}
	public LongWritable getC2() {
		return C2;
	}
	public void setC2(LongWritable c2) {
		C2 = c2;
	}
}





