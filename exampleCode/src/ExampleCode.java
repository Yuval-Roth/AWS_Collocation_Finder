import com.amazonaws.auth.*;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;

public class ExampleCode {

    private static final String CREDENTIALS_PATH = getFolderPath() + "credentials.txt";

    public static void main(String[] args) {

        AWSCredentialsProvider provider ;
        try {
            AwsCredentialsReader credsReader = new AwsCredentialsReader(CREDENTIALS_PATH);
            provider = credsReader.getCredentialsProvider();
        } catch (AwsCredentialsReader.CredentialsReaderException e) {
            throw new RuntimeException(e);
        }
        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClient.builder()
                .withCredentials(provider)
                .withRegion("us-east-1")
                .build();

        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3n://yourbucket/yourfile.jar") // This should be a full map reduce application.
                .withMainClass("some.pack.MainClass")
                .withArgs("s3n://yourbucket/input/", "s3n://yourbucket/output/");

        StepConfig stepConfig = new StepConfig()
                .withName("stepname")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(2)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.6.0").withEc2KeyName("yourkey")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("jobname")
                .withInstances(instances)
                .withSteps(stepConfig)
                .withLogUri("s3n://yourbucket/logs/");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }

    private static String getFolderPath() {
        String folderPath = ExampleCode.class.getResource("ClientMainClass.class").getPath();
        folderPath = folderPath.replace("%20"," "); //fix space character
        folderPath = folderPath.substring(folderPath.indexOf("/")+1); // remove initial '/'
        folderPath = folderPath.substring(0,folderPath.lastIndexOf("/")); // remove .class file from path
        folderPath = folderPath.substring(0,folderPath.lastIndexOf("/")+1); // exit jar
        return folderPath;
    }
}
