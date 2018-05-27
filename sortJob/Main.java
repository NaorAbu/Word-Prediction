package ass2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

	public static class MapperClass extends Mapper<Text,Text, sortKey, Text> {
		@Override
		public void map(Text key,Text value, Context context) throws IOException,  InterruptedException {
				context.write(new sortKey(key,new DoubleWritable(Double.parseDouble(value.toString()))),key);
		}
	}

	public static class ReducerClass extends Reducer<sortKey,Text,Text,Text> {
		@Override
		public void reduce(sortKey key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
			for (Text value : values) {
				context.write(value,new Text(key.getC().toString()));
			}
		}
	}

	public static class PartitionerClass extends Partitioner<sortKey, Text> {
		@Override
		 public int getPartition(sortKey key, Text val, int numPartitions) {
	        int hash = Math.abs(key.getW().hashCode());
	        int partition = hash % numPartitions;
	        return partition;
	    } 
	}
	
	public static void main(String [] args) {
		Configuration job4conf = new Configuration();
		System.out.println("started sort Job");
		Job job4;
		try {
			job4 = new Job(job4conf, "sortJob");
			job4.setJarByClass(Main.class);
			job4.setPartitionerClass(PartitionerClass.class);
			job4.setMapperClass(MapperClass.class);
			job4.setReducerClass(ReducerClass.class);
			job4.setSortComparatorClass(sortKeyComparator.class);

			job4.setMapOutputKeyClass(sortKey.class);
			job4.setMapOutputValueClass(Text.class);

			job4.setOutputKeyClass(Text.class);
			job4.setOutputValueClass(Text.class);

			job4.setInputFormatClass(KeyValueTextInputFormat.class);
			job4.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);

			FileInputFormat.addInputPath(job4, new Path("INPUT PATH FROM mergeJob OUTPUT/part*"));
			FileOutputFormat.setOutputPath(job4, new Path("OUTPUT PATH"));
			try {
				System.exit(job4.waitForCompletion(true) ? 0 : 1);
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
