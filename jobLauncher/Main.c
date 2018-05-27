package ass2;


import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

public class Main {
/* This is just the general template for job flow
   Every one of 4 steps need to upload to server as maven build jar file 
   I uses AWS S3 to upload the jars
   All the linking here are missing
   All the setting like Instances type and etc can be changed to your choices
   */
   
	public static void main(String[] args) {
		BasicAWSCredentials credentials = new BasicAWSCredentials("INSERT", "CREDENTIALS");//need to fill in private credentials
		AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);

		HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
				.withJar("nGramJob maven build jar link here") // This should be a full map reduce application.
				.withMainClass("ass2.Main")
				.withArgs();

		StepConfig stepConfig1 = new StepConfig()
				.withName("Step 1")
				.withHadoopJarStep(hadoopJarStep1)
				.withActionOnFailure("TERMINATE_JOB_FLOW");
	
		HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()
				.withJar("splitJob maven build jar link here ") // This should be a full map reduce application.
				.withMainClass("ass2.Main")
				.withArgs();

		StepConfig stepConfig2 = new StepConfig()
				.withName("Step 2")
				.withHadoopJarStep(hadoopJarStep2)
				.withActionOnFailure("TERMINATE_JOB_FLOW");
			*/
		HadoopJarStepConfig hadoopJarStep3 = new HadoopJarStepConfig()
				.withJar("mergeJob maven build jar link here") // This should be a full map reduce application.
				.withMainClass("ass2.Main")
				.withArgs();

		StepConfig stepConfig3 = new StepConfig()
				.withName("Step 3")
				.withHadoopJarStep(hadoopJarStep3)
				.withActionOnFailure("TERMINATE_JOB_FLOW");
		
		HadoopJarStepConfig hadoopJarStep4 = new HadoopJarStepConfig()
				.withJar("sortJob maven build jar link here") // This should be a full map reduce application.
				.withMainClass("ass2.Main")
				.withArgs();

		StepConfig stepConfig4 = new StepConfig()
				.withName("Step 4")
				.withHadoopJarStep(hadoopJarStep4)
				.withActionOnFailure("TERMINATE_JOB_FLOW");

		JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
				.withInstanceCount(4)
				.withMasterInstanceType(InstanceType.C1Xlarge.toString())
				.withSlaveInstanceType(InstanceType.C1Xlarge.toString())
				.withHadoopVersion("2.8.2").withEc2KeyName("EC2 KEY NAME")//private key name need to be insert
				.withKeepJobFlowAliveWhenNoSteps(false)
				.withPlacement(new PlacementType("us-east-1a"));//you can choose any region you like 

		RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
				.withName("Assignment2")
				.withInstances(instances)
				.withReleaseLabel("emr-5.11.0")
				.withSteps(stepConfig3,stepConfig4)
				.withJobFlowRole("EMR_EC2_DefaultRole")
				.withServiceRole("EMR_DefaultRole")
				.withLogUri("link to save logs *optional*");
		
		RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
		String jobFlowId = runJobFlowResult.getJobFlowId();
		System.out.println("Ran job flow with id: " + jobFlowId);
	}
}
