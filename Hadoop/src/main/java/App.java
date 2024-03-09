import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import exceptions.AWSCredentialsReaderException;

public class App {

    // <S3>
    public static final String BUCKET_NAME = "distributed-systems-2024-bucket-yuval-adi";
    public static final String BUCKET_URL = "s3://" + BUCKET_NAME + "/";
    // </S3>

    // <APPLICATION DATA>
    private static final int INSTANCE_COUNT = 1;
    public static final String HADOOP_JARS_URL = BUCKET_URL + "hadoop/jars/";
    public static final String HADOOP_OUTPUTS_URL = BUCKET_URL + "hadoop/outputs/";
    public static final String JAR_STEP_ARGS = "%s %s -inputUrl %s -outputUrl %s";
    private static final String CREDENTIALS_PATH = getFolderPath() + "credentials.txt";
    private static final String STOP_WORDS_FILE = "stop_words.txt";
    private static final String USAGE = ""; // TODO: WRITE USAGE
    public static String inputUrl;
    public static String outputUrl;
    private static Double minPmi;
    private static Double relMinPmi;
    private static boolean debug;
    // </APPLICATION DATA>
    public static AWSCredentialsProvider credentialsProvider;
    public static AmazonS3 S3;
    public static AmazonEC2 ec2;
    public static AmazonElasticMapReduce emr;

    public static int numberOfInstances = 1;

    public static void main(String[]args) throws AWSCredentialsReaderException {
        credentialsProvider = new AWSCredentialsReader(CREDENTIALS_PATH);

        System.out.println("[INFO] Connecting to aws");
        ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        S3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        System.out.println( "list cluster");
        System.out.println( emr.listClusters());

        // Step 1
        HadoopJarStepConfig step1 = new HadoopJarStepConfig()
                .withJar("s3://distributed-systems-2024-bucket-yuval-adi/hadoop/jars/WordCount.jar")
                .withMainClass("WordCount");

        StepConfig stepConfig1 = new StepConfig()
                .withName("Step1")
                .withHadoopJarStep(step1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        //Job flow
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(numberOfInstances)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.9.2")
                .withEc2KeyName("vockey")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        System.out.println("Set steps");
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Map reduce project")
                .withInstances(instances)
                .withSteps(stepConfig1)
                .withLogUri("s3://distributed-systems-2024-bucket-yuval-adi/hadoop/logs/")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0");

        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }

    private static String getFolderPath() {
        String folderPath = WordCount.class.getResource("WordCount.class").getPath();
        folderPath = folderPath.replace("%20"," "); //fix space character
        folderPath = folderPath.substring(folderPath.indexOf("/")+1); // remove initial '/'
        folderPath = folderPath.substring(0,folderPath.lastIndexOf("/")); // remove .class file from path
        folderPath = folderPath.substring(0,folderPath.lastIndexOf("/")+1); // exit jar
        return folderPath;
    }
}
