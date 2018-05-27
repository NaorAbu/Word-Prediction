package ass2;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class Main {

	public static class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
			String[] split = value.toString().split("\t");
			String keyWord = split[0];
			if (filter(split[0])) {
				context.write(new Text(keyWord), new LongWritable(Long.parseLong(split[2])));
				String[] words = keyWord.split("\\s+");
				if (words.length==1) context.write(new Text("c0Counter"), new LongWritable(Long.parseLong(split[2])));
			}
			
		}

		public boolean filter(String s) {
			String s1 = s.replaceAll("\\p{L}","");
			String s2 = s1.replaceAll("\\p{Z}", "");
			return (s2.length()==0);
		}

	}



	public static class CombinerClass extends Reducer<Text,LongWritable,Text,LongWritable> {
		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
			long sum = 0;
			for (LongWritable value : values) {
				sum += Math.abs(value.get());
			}
			context.write(key, new LongWritable(sum)); 
		}
	}


	public static class ReducerClass extends Reducer<Text,LongWritable,Text,LongWritable> {
		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
			long sum = 0;
			for (LongWritable value : values) {
				sum += Math.abs(value.get());
			}
			if (key.equals(new Text("c0Counter"))) {
				c0Upload(sum);
			}else {
				context.write(key, new LongWritable(sum)); 
			}
		}

		public void c0Upload(long sum) {
    /*C0 is major parmater for the total counting, after been counted uploaed to any data base
    In this example its upload to AWS S3 bucket in the next steps need to download it back*/
			BasicAWSCredentials credentials = new BasicAWSCredentials("PRIVATE", "CREDENTIALS");
			AmazonS3 s3 = AmazonS3ClientBuilder.standard()
					.withCredentials(new AWSStaticCredentialsProvider(credentials))
					.withRegion("us-east-1") 
					.build();
			String str = "c0counter";
			try {
				PrintWriter writer = new PrintWriter(str, "UTF-8");
				writer.println(sum);
				writer.close();
				File file = new File(str);
				s3.putObject(new PutObjectRequest(str, str, file));

			} catch (AmazonServiceException ase) {
				System.out.println("Caught an AmazonServiceException, which " +
						"means your request made it " +
						"to Amazon S3, but was rejected with an error response" +
						" for some reason.");
				System.out.println("Error Message:    " + ase.getMessage());
				System.out.println("HTTP Status Code: " + ase.getStatusCode());
				System.out.println("AWS Error Code:   " + ase.getErrorCode());
				System.out.println("Error Type:       " + ase.getErrorType());
				System.out.println("Request ID:       " + ase.getRequestId());
			} catch (AmazonClientException ace) {
				System.out.println("Caught an AmazonClientException, which " +
						"means the client encountered " +
						"an internal error while trying to " +
						"communicate with S3, " +
						"such as not being able to access the network.");
				System.out.println("Error Message: " + ace.getMessage());
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static class PartitionerClass extends Partitioner<Text, LongWritable> {
		@Override
		public int getPartition(Text key, LongWritable value, int numPartitions) {
			return Math.abs(key.hashCode()) % numPartitions;
		}    
	}


	public static void main(String[] args) {
		//initAws();
		System.out.println("started ngramJob");
		Configuration job1conf = new Configuration();

		Job job1;
		try {
			job1 = new Job(job1conf, "nGramJob");
			job1.setJarByClass(Main.class);
			job1.setPartitionerClass(PartitionerClass.class);
			job1.setMapperClass(MapperClass.class);
			job1.setReducerClass(ReducerClass.class);
			job1.setCombinerClass(CombinerClass.class);
			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(LongWritable.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(LongWritable.class);
			job1.setInputFormatClass(SequenceFileInputFormat.class);
			job1.setOutputFormatClass(TextOutputFormat.class);

			MultipleInputs.addInputPath(job1,new Path("s3n://datasets.elasticmapreduce/ngrams/books/20090715/eng-all/1gram/data"), SequenceFileInputFormat.class);
			MultipleInputs.addInputPath(job1,new Path("s3n://datasets.elasticmapreduce/ngrams/books/20090715/eng-all/2gram/data"), SequenceFileInputFormat.class);
			MultipleInputs.addInputPath(job1,new Path("s3n://datasets.elasticmapreduce/ngrams/books/20090715/eng-all/3gram/data"), SequenceFileInputFormat.class);
			FileOutputFormat.setOutputPath(job1, new Path("link to output = link to next job input));
			try {
				int exitCode =  job1.waitForCompletion(true) ? 0 : 1;
				System.exit(exitCode);
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
