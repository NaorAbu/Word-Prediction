package ass2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;

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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
/*
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
*/
public class Main { 
	public static class MapperClass extends Mapper<Text,Text, Text, nGramWritable> {
		@Override
		public void map(Text key,Text value, Context context) throws IOException,  InterruptedException {
			String[] split = value.toString().split(" ");
			context.write(key,new nGramWritable(new LongWritable(Long.parseLong(split[0])),new IntWritable(Integer.parseInt(split[1]))));	
		}
	}

	public static class ReducerClass extends Reducer<Text,nGramWritable,Text,Text> {
		@Override
		public void reduce(Text key, Iterable<nGramWritable> values, Context context) throws IOException,  InterruptedException {
			System.out.println("REDUCER:"+ context.getConfiguration().get("C0"));
			nGramWritable out = new nGramWritable();
			for (nGramWritable value : values) {
				out.merge(value);
			}
			context.write(key, new Text((out.prob(context.getConfiguration().get("C0").toString())).toString()));
		}

	}
	
	public static class CombinerClass extends Reducer<Text,nGramWritable,Text,nGramWritable> {
		@Override
		public void reduce(Text key, Iterable<nGramWritable> values, Context context) throws IOException,  InterruptedException {
			nGramWritable out = new nGramWritable();
			for (nGramWritable value : values) {
				out.merge(value);
			}
			context.write(key, out);
		}

	}

	public static class PartitionerClass extends Partitioner<Text, nGramWritable> {
		@Override
		public int getPartition(Text key, nGramWritable value, int numPartitions) {
			return Math.abs(key.hashCode()) % numPartitions;
		}    
	}

	
	
	public static void main(String[]args) throws IOException {
		System.out.println("Started Merge Job");
		BasicAWSCredentials credentials = new BasicAWSCredentials("PRIVATE", "CREDENTIALS");
		AmazonS3 s3 = AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(credentials))
				.withRegion("us-east-1") 
				.build();
		String str = "c0counter";
		S3Object o = s3.getObject(str,str);
		BufferedReader reader = new BufferedReader(new InputStreamReader(o.getObjectContent()));
		String c0 = reader.readLine().toString();
		System.out.println(c0);
		Configuration job3conf = new Configuration();
		job3conf.set("C0", c0);
		Job job3;
		try {
			job3 = new Job(job3conf, "mergeJob");
			job3.setJarByClass(Main.class);
			job3.setPartitionerClass(PartitionerClass.class);
			job3.setMapperClass(MapperClass.class);
			job3.setCombinerClass(CombinerClass.class);
			job3.setReducerClass(ReducerClass.class);
			job3.setMapOutputKeyClass(Text.class);
			job3.setMapOutputValueClass(nGramWritable.class);
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(Text.class);
			job3.setInputFormatClass(KeyValueTextInputFormat.class);
			FileInputFormat.addInputPath(job3, new Path("s3n://outputjob/outputjob2/part*"));
			FileOutputFormat.setOutputPath(job3, new Path("s3n://outputjob/outputjob3/"));
			try {
				System.exit(job3.waitForCompletion(true) ? 0 : 1);
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
