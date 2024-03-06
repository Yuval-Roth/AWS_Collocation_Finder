import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import exceptions.AWSCredentialsReaderException;
import utils.AWSCredentialsReader;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class CollocationFinder {

    // <S3>
    public static final String BUCKET_NAME = "distributed-systems-2024-bucket-yuval-adi";
    public static final String BUCKET_URL = "s3://" + BUCKET_NAME + "/";
    // </S3>

    // <APPLICATION DATA>
    private static final int INSTANCE_COUNT = 7;
    public static final String HADOOP_JARS_URL = BUCKET_URL + "hadoop/jars/";
    public static final String HADOOP_OUTPUTS_URL = BUCKET_URL + "hadoop/outputs/";
    public static final String JAR_STEP_ARGS = "%s -inputUrl %s -outputUrl %s";
    private static final String CREDENTIALS_PATH = getFolderPath() + "credentials.txt";
    private static final String STOP_WORDS_FILE = "stop_words.txt";
    private static final String USAGE = ""; // TODO: WRITE USAGE
    public static String inputUrl;
    public static String outputUrl;
    private static Double minPmi;
    private static Double relMinPmi;
    // </APPLICATION DATA>

    public static void main(String[] args) {

        readArgs(args);

        AWSCredentialsProvider credentialsProvider;

        try {
            credentialsProvider = new AWSCredentialsReader(CREDENTIALS_PATH);
        } catch (AWSCredentialsReaderException e) {
            System.out.println(e.getMessage());
            return;
        }

        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClient.builder()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();

        List<StepConfig> stepConfigs = new LinkedList<>();
        String[] firstArg = {
                "-stopWordsFile "+ STOP_WORDS_FILE,
                "",
                "-minPmi "+minPmi,
                "-relMinPmi "+relMinPmi
        };
        String output = HADOOP_OUTPUTS_URL + UUID.randomUUID();
        String input = inputUrl;
        for(int i = 1 ; i <= 4 ; i++){
            String _args = JAR_STEP_ARGS.formatted(firstArg[i - 1], input, output);
            HadoopJarStepConfig step = new HadoopJarStepConfig()
                    .withJar(HADOOP_JARS_URL+"step"+ i + ".jar")
                    .withMainClass("Step" + i)
                    .withArgs(_args.split(" "));
            input = output;
            output = HADOOP_OUTPUTS_URL + UUID.randomUUID();
            stepConfigs.add(new StepConfig()
                    .withName("step"+i)
                    .withHadoopJarStep(step)
                    .withActionOnFailure("TERMINATE_JOB_FLOW"));
        }
        outputUrl = output;
        System.out.println("output file will be located at "+ outputUrl);

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(INSTANCE_COUNT)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("3.3.6").withEc2KeyName("vockey")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Collocation Finder")
                .withInstances(instances)
                .withSteps(stepConfigs)
                .withLogUri(BUCKET_URL+"hadoop/logs/")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-7.0.0");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }

    private static String getFolderPath() {
        String folderPath = CollocationFinder.class.getResource("CollocationFinder.class").getPath();
        folderPath = folderPath.replace("%20"," "); //fix space character
        folderPath = folderPath.substring(folderPath.indexOf("/")+1); // remove initial '/'
        folderPath = folderPath.substring(0,folderPath.lastIndexOf("/")); // remove .class file from path
        folderPath = folderPath.substring(0,folderPath.lastIndexOf("/")+1); // exit jar
        return folderPath;
    }

    private static void printUsageAndExit(String errorMessage) {
        if(! errorMessage.equals("")) {
            System.out.println(errorMessage);
        }
        System.out.println(USAGE);
        System.exit(1);
    }

    private static void readArgs(String[] args) {

        List<String> argsList = new LinkedList<>();
        argsList.add("-inputurl");
        argsList.add("-minpmi");
        argsList.add("-relminpmi");
        for (int i = 0; i < args.length; i++) {
            String arg = args[i].toLowerCase();
            String errorMessage;

            if (arg.equals("-inputurl")) {
                errorMessage = "Missing input url\n";
                try{
                    if(argsList.contains(args[i+1])){
                        printUsageAndExit(errorMessage);
                    }
                    inputUrl = args[i+1];
                    i++;
                    continue;
                } catch (IndexOutOfBoundsException e){
                    System.out.println();
                    printUsageAndExit(errorMessage);
                }
            }

            if (arg.equals("-minpmi")) {
                errorMessage = "Missing minimum pmi\n";
                try{
                    if(argsList.contains(args[i+1])){
                        printUsageAndExit(errorMessage);
                    }
                    try{
                        minPmi = Double.parseDouble(args[i+1]);
                    } catch (NumberFormatException e2){
                        System.out.println();
                        printUsageAndExit("Invalid minimum pmi\n");
                    }
                    if(minPmi <= 0) {
                        System.out.println();
                        printUsageAndExit("Invalid minimum pmi\n");
                    }
                    i++;
                    continue;
                } catch (IndexOutOfBoundsException e){
                    System.out.println();
                    printUsageAndExit(errorMessage);
                }
            }

            if (arg.equals("-relminpmi")) {
                errorMessage = "Missing relative minimum pmi\n";
                try{
                    if(argsList.contains(args[i+1])){
                        printUsageAndExit(errorMessage);
                    }
                    try{
                        relMinPmi = Double.parseDouble(args[i+1]);
                    } catch (NumberFormatException e2){
                        System.out.println();
                        printUsageAndExit("Invalid relative minimum pmi\n");
                    }
                    if(relMinPmi <= 0) {
                        System.out.println();
                        printUsageAndExit("Invalid relative minimum pmi\n");
                    }
                    i++;
                    continue;
                } catch (IndexOutOfBoundsException e){
                    System.out.println();
                    printUsageAndExit(errorMessage);
                }
            }

            System.out.println();
            printUsageAndExit("Unknown argument: %s\n".formatted(arg));
        }

        if(minPmi == null){
            printUsageAndExit("Argument for minimum pmi not found\n");
        }
        if(relMinPmi == null){
            printUsageAndExit("Argument for relative minimum pmi not found\n");
        }
        if(inputUrl == null){
            printUsageAndExit("Argument for input url not found\n");
        }
    }
}
