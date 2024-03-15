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

    enum Language {
        english,
        hebrew
    }
    // <S3>
    public static final String BUCKET_NAME = "distributed-systems-2024-bucket-yuval-adi";
    public static final String BUCKET_URL = "s3://" + BUCKET_NAME + "/";
    public static final String HADOOP_JARS_URL = BUCKET_URL + "hadoop/jars/";
    public static final String HADOOP_OUTPUTS_URL = BUCKET_URL + "hadoop/outputs/";
    // </S3>

    // <CONSTANTS>
    public static final String JAR_STEP_ARGS = "%s -inputUrl %s -outputUrl %s";
    private static final String CREDENTIALS_PATH = getFolderPath() + "credentials.txt";
    private static final String USAGE = """
    Usage: java -jar CollocationFinder.jar -inputUrl <inputUrl> -minPmi <minPmi> -relMinPmi <relMinPmi>
         -language <language> -instanceCount <instanceCount> [-compressed] [-corpusPercentage <corpusPercentage>]
    
    [-h | -help] := prints this message
    -compressed := uses SequenceFileInputFormat when reading the input
    -corpusPercentage <corpusPercentage> := the percentage of the corpus to use (default is 1.0)
                                            use a value between 0.0 and 1.0
    -language <language> := the language of the input corpus (either 'english' or 'hebrew')""";
    // </CONSTANTS>

    // <APPLICATION DATA>
    public static String inputUrl;
    private static Double minPmi;
    private static Double relMinPmi;
    private static Integer instanceCount;
    private static Language language;
    private static String stopWordsFile;
    private static Double corpusPercentage;
    private static boolean compressed;
    private static Long splitSize;
    // </APPLICATION DATA>

    public static void main(String[] args) {

        readArgs(args);

        stopWordsFile = language.equals(Language.english) ? "eng_stop_words.txt" : "heb_stop_words.txt";

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
                "-stopWordsFile %s -language %s -corpusPercentage %s %s %s".formatted(
                        stopWordsFile, language, corpusPercentage,
                        compressed ? "-compressed" : "",
                        splitSize == null ? "" : "-splitSize %d".formatted(splitSize)),
                "",
                "-relMinPmi %f -minPmi %f".formatted(relMinPmi, minPmi)
        };
        String output = HADOOP_OUTPUTS_URL + UUID.randomUUID();
        String input = inputUrl;
        int jobsCount = 3;
        for(int i = 1; i <= jobsCount; i++){
            String _args = JAR_STEP_ARGS.formatted(firstArg[i - 1], input, output);
            HadoopJarStepConfig step = new HadoopJarStepConfig()
                    .withJar(HADOOP_JARS_URL+"step"+ i + ".jar")
                    .withMainClass("Step" + i)
                    .withArgs(_args.split(" "));
            System.out.println("output file for step "+i+" will be located at "+ output);
            input = output;
            if(i != jobsCount) output = HADOOP_OUTPUTS_URL + UUID.randomUUID();
            stepConfigs.add(new StepConfig()
                    .withName("step"+i)
                    .withHadoopJarStep(step)
                    .withActionOnFailure("TERMINATE_JOB_FLOW"));
        }
        System.out.println("\nfinal output file will be located at "+ output);

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(instanceCount)
//                .withMasterInstanceType(InstanceType.M4Large.toString())
//                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withMasterInstanceType(InstanceType.M4Xlarge.toString())
                .withSlaveInstanceType(InstanceType.M4Xlarge.toString())
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
        argsList.add("-h");
        argsList.add("-help");
        argsList.add("-inputurl");
        argsList.add("-minpmi");
        argsList.add("-relminpmi");
        argsList.add("-corpuspercentage");
        argsList.add("-language");
        argsList.add("-compressed");
        argsList.add("-instancecount");
        for (int i = 0; i < args.length; i++) {
            String arg = args[i].toLowerCase();
            String errorMessage;
            if(arg.equals("-instancecount")){
                errorMessage = "Missing instance count\n";
                try{
                    if(argsList.contains(args[i+1])){
                        printUsageAndExit(errorMessage);
                    }
                    instanceCount = Integer.valueOf(args[i+1]);
                    i++;
                    continue;
                } catch (NumberFormatException e){
                    System.out.println();
                    printUsageAndExit("Invalid instance count\n");
                }
            }
            if(arg.equals("-splitsize")){
                errorMessage = "Bad split size argument\n";
                try{
                    if(argsList.contains(args[i+1])){
                        printUsageAndExit(errorMessage);
                    }
                    splitSize = Long.valueOf(args[i+1]);
                    i++;
                    continue;
                } catch (NumberFormatException e){
                    System.out.println();
                    printUsageAndExit("Invalid instance count\n");
                }
            }
            if(arg.equals("-corpuspercentage")){
                errorMessage = "Missing corpus percentage\n";
                try{
                    if(argsList.contains(args[i+1])){
                        printUsageAndExit(errorMessage);
                    }
                    corpusPercentage = Double.valueOf(args[i+1]);
                    i++;
                    continue;
                } catch (NumberFormatException e){
                    System.out.println();
                    printUsageAndExit("Invalid corpus percentage\n");
                }
            }
            if (arg.equals("-compressed")){
                compressed = true;
                continue;
            }
            if (arg.equals("-language")) {
                errorMessage = "Bad language argument\n";
                try{
                    if(argsList.contains(args[i+1])){
                        printUsageAndExit(errorMessage);
                    }
                    language = Language.valueOf(args[i+1]);
                    i++;
                    continue;
                } catch (IllegalArgumentException e){
                    System.out.println();
                    printUsageAndExit("Invalid language. either 'english' or 'hebrew' are available\n");
                }
            }
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
                    if(minPmi < 0) {
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
                    if(relMinPmi < 0) {
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
            if(arg.equals("-h") || arg.equals("-help")){
                printUsageAndExit("");
            }
            System.out.println();
            printUsageAndExit("Unknown argument: %s\n".formatted(arg));
        }

        if(corpusPercentage == null){
            corpusPercentage = 1.0;
        }

        if(corpusPercentage < 0.0 || corpusPercentage > 1.0){
            printUsageAndExit("Corpus percentage must be between 0 and 1\n");
        }
        if(instanceCount == null){
            printUsageAndExit("Argument for instance count not found\n");
        }
        if(language == null){
            printUsageAndExit("Argument for language not found\n");
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
