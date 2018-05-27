package ass2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
	public static class MapperClass extends Mapper<Text,Text, compositeKey, pairWritable> {
		@Override
		public void map(Text key,Text value, Context context) throws IOException,  InterruptedException {
			String [] split = key.toString().split(" ");
			if (split.length == 3) {// if its of type W1W2W3
				String w1 = split[0];
				String w2 = split[1];
				String w3 = split[2];
				context.write(new compositeKey(new Text(w2),false), new pairWritable(key,new IntWritable(1)));//C1
				context.write(new compositeKey(new Text(w3),false), new pairWritable(key,new IntWritable(2)));//N1
				context.write(new compositeKey(new Text(w1 + " " + w2),false), new pairWritable(key,new IntWritable(3)));//C2
				context.write(new compositeKey(new Text(w2 + " " + w3),false), new pairWritable(key,new IntWritable(4)));//N2
				context.write(new compositeKey(key,false), new pairWritable(key,new IntWritable(5)));//N3
			}
			context.write(new compositeKey(key,true), new pairWritable(key,value));

		}
	}
	public static class ReducerClass extends Reducer<compositeKey,pairWritable,Text,countTypeWritable> {
		@Override
		public void reduce(compositeKey key, Iterable<pairWritable> values, Context context) throws IOException,  InterruptedException {
			LongWritable count = new LongWritable(0);
			boolean flag = true;
			for (pairWritable value : values) {
				if(flag)	{
					count.set(value.getC().get());
					flag=false;
				}else {
					IntWritable type = value.getType();
					context.write(value.getW(), new countTypeWritable(new LongWritable(count.get()),type));
				}
			}
		}

	}


	
	public static class PartitionerClass extends Partitioner<compositeKey, pairWritable> {
		@Override
		  public int getPartition(compositeKey key, pairWritable val, int numPartitions) {
	        int hash = Math.abs(key.getW().hashCode());
	        int partition = hash % numPartitions;
	        return partition;
	    }  
	}
	

	public static void main(String[]args) {
		Configuration job2conf =new Configuration();
		System.out.println("Started split job");
		Job job2;
		try { 
			job2 = new Job(job2conf, "splitJob");
			job2.setJarByClass(Main.class);
			job2.setPartitionerClass(PartitionerClass.class);
			job2.setMapperClass(MapperClass.class);
			job2.setReducerClass(ReducerClass.class);
			job2.setSortComparatorClass(CompositeKeyComparator.class);

			job2.setMapOutputKeyClass(compositeKey.class);
			job2.setMapOutputValueClass(pairWritable.class);

			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(countTypeWritable.class);

			job2.setInputFormatClass(KeyValueTextInputFormat.class);
			job2.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
			FileInputFormat.addInputPath(job2, new Path("INPUT FROM nGRAM OUTPUT/part*"));
			FileOutputFormat.setOutputPath(job2, new Path("OUTPUT LINK = INPUT FOR THE NEXT JOB"));
			try {
				System.out.println("Before wait for complete");
				System.exit(job2.waitForCompletion(true) ? 0 : 1);
				System.out.println("After wait for complete");
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
