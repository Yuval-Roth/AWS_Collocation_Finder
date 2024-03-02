import com.amazonaws.auth.*;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.polly.model.InvalidS3KeyException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import exceptions.AWSCredentialsReaderException;
import exceptions.TerminateException;
import utils.Box;

import java.io.*;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class ExampleCode {

    // <S3>
    public static final String BUCKET_NAME = "distributed-systems-2024-bucket-yuval-adi";
    private static AmazonS3 s3;
    // </S3>

    // <EC2>
    private static AmazonEC2 ec2;
    public static String WORKER_IMAGE_ID;
    public static final String SECURITY_GROUP = "sg-00c67312e0a74a525";
    public static final String WORKER_INSTANCE_TYPE = "t2.large";
    // </EC2>


    // <SQS>
    private static final String SQS_DOMAIN_PREFIX = "https://sqs.us-east-1.amazonaws.com/057325794177/";
    // </SQS>

    // <DEBUG FLAGS>
    //TODO: Change the usage string to match the program
    private static final String USAGE = """
                Usage: java -jar managerProgram.jar [-h | -help] [optional debug flags]
                                    
                -h | -help :- Print this message and exit.
                                    
                optional debug flags:
                
                    -d | -debug :- Run in debug mode, logging all operations to standard output
                    
                    -ul | -uploadLog <file name> :- logs will be uploaded to <file name> in the S3 bucket.
                                  Must be used with -debug.
                                  
                    -ui | -uploadInterval <interval in seconds> :- When combined with -uploadLog, specifies the interval in seconds
                                  between log uploads to the S3 bucket.
                                  Must be a positive integer, must be used with -uploadLog.
                                  If this argument is not specified, defaults to 60 seconds.
                                  
                    -noEc2 :- Run without creating EC2 instances. Useful for debugging locally.
                    
                credentials for aws:
                    The program will use the default aws credentials provider chain to get credentials.
                    We recommend using environment variables to set credentials.
                """;
    private static volatile boolean debugMode;
    private static volatile boolean noEc2;
    private static volatile boolean uploadLogs;
    private static volatile int appendLogIntervalInSeconds;
    private static volatile StringBuilder uploadBuffer;
    private static volatile long nextLogUpload;
    private static String uploadLogName;
    // </DEBUG FLAGS>

    // <APPLICATION DATA>
    //TODO: Change CLASS_NAME to the name of the class
    private static final String CLASS_NAME = "ExampleCode";
    private static final String CREDENTIALS_PATH = getFolderPath() + "credentials.txt";
    private static final Regions ec2_region = Regions.US_EAST_1;
    private static final Regions s3_region = Regions.US_WEST_2;
    private static ThreadPoolExecutor executor;
    // </APPLICATION DATA>


    public static void main(String[] args) {

        AWSCredentialsProvider credentialsProvider ;
        try {
            credentialsProvider = new AWSCredentialsReader(CREDENTIALS_PATH);
        } catch (AWSCredentialsReaderException e) {
            System.out.println(e.getMessage());
            return;
        }

        s3 = AmazonS3Client.builder()
                .withCredentials(credentialsProvider)
                .withRegion(s3_region)
                .build();

        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClient.builder()
                .withCredentials(credentialsProvider)
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

    // ============================================================================ |
    // ========================  AWS API FUNCTIONS  =============================== |
    // ============================================================================ |

    private static String downloadSmallFileFromS3(String key) {

        var r = s3.getObject(new GetObjectRequest(BUCKET_NAME, "files/"+key));

        // get file from response
        byte[] file = {};
        try {
            file = r.getObjectContent().readAllBytes();
        } catch (IOException e) {
            handleException(e);
        }

        return new String(file);
    }

    /**
     * Downloads a file from S3 to a local temporary file and returns the file
     * @param key - the key of the file in S3
     * @return the local temporary file that was downloaded. the file should be deleted after use.
     */
    private static File downloadBigFileFromS3(String key){
        String localPath = getFolderPath() + key + ".tmp";
        File file = new File(localPath);
        s3.getObject(new GetObjectRequest(BUCKET_NAME, "files/"+key), file);
        return file;
    }

    private static void uploadSmallFileToS3(String key, String content) {
        InputStream input = new ByteArrayInputStream(content.getBytes());
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(content.length());
        s3.putObject(new PutObjectRequest(BUCKET_NAME, "files/"+key, input, metadata));
    }

    private static void uploadBigFileToS3(String key, File contentFile) throws FileNotFoundException {
        long contentLength = contentFile.length();
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(contentLength);
        BufferedInputStream input = new BufferedInputStream(new FileInputStream(contentFile));
        s3.putObject(new PutObjectRequest(BUCKET_NAME, "files/"+key, input, metadata));
    }

    private static void appendToSmallFileS3(String key, String content) {
        String oldContent = "";
        try{
            oldContent = downloadSmallFileFromS3(key);
        } catch (InvalidS3KeyException ignored){}
        uploadSmallFileToS3(key, oldContent + content);
    }

    /**
     *
     * @throws IOException if there is an error reading the file
     * @throws FileNotFoundException if the file is not found
     */
    private static void appendToBigFileS3(String key, File contentToAppendFile) throws IOException, FileNotFoundException {
        File downloadedFile = downloadBigFileFromS3(key);

        // append content to file
        BufferedWriter writer = new BufferedWriter(new FileWriter(downloadedFile,true));
        BufferedReader contentFileStream = new BufferedReader(new FileReader(contentToAppendFile));
        String line;
        while((line = contentFileStream.readLine()) != null){
            writer.write(line);
            writer.newLine();
        }
        writer.close();
        contentFileStream.close();

        // upload file
        uploadBigFileToS3(key, downloadedFile);

        // delete local temporary file
        downloadedFile.delete();
    }

    // ============================================================================ |
    // ========================  UTILITY FUNCTIONS  =============================== |
    // ============================================================================ |

    private static void handleException(Exception e) {
        LocalDateTime now = LocalDateTime.now();

        if(e instanceof TerminateException){
            /*
             * Termination code goes here
             */
            System.exit(0);
        }

        // TODO: Change logName to the name of the log file
        String logName = "FILL THIS";
        String timeStamp = getTimeStamp(now);
        String logS3Path = "errors/%s %s.log".formatted(timeStamp,logName);

        String stackTrace = stackTraceToString(e);
        uploadSmallFileToS3(logS3Path,stackTrace);
        log("Exception in thread %s:\n%s".formatted(Thread.currentThread(),stackTraceToString(e)));
    }

    private static String getTimeStamp(LocalDateTime now) {
        return "[%s.%s.%s - %s:%s:%s]".formatted(
                now.getDayOfMonth() > 9 ? now.getDayOfMonth() : "0"+ now.getDayOfMonth(),
                now.getMonthValue() > 9 ? now.getMonthValue() : "0"+ now.getMonthValue(),
                now.getYear(),
                now.getHour() > 9 ? now.getHour() : "0"+ now.getHour(),
                now.getMinute() > 9 ? now.getMinute() : "0"+ now.getMinute(),
                now.getSecond() > 9 ? now.getSecond() : "0"+ now.getSecond());
    }

    private static String getFolderPath() {
        String folderPath = ExampleCode.class.getResource("%s.class".formatted(CLASS_NAME)).getPath();
        folderPath = folderPath.replace("%20"," "); //fix space character
        folderPath = folderPath.substring(folderPath.indexOf("/")+1); // remove initial '/'
        folderPath = folderPath.substring(0,folderPath.lastIndexOf("/")); // remove .class file from path
        folderPath = folderPath.substring(0,folderPath.lastIndexOf("/")+1); // exit jar
        return folderPath;
    }

    private static String stackTraceToString(Exception e) {
        StringBuilder output  = new StringBuilder();
        output.append(e).append("\n");
        for (var element: e.getStackTrace()) {
            output.append("\t").append(element).append("\n");
        }
        return output.toString();
    }

    private static void printUsageAndExit(String errorMessage) {
        if(! errorMessage.equals("")) {
            System.out.println(errorMessage);
        }
        System.out.println(USAGE);
        System.exit(1);
    }

    private static void atomicIntegerAdd(AtomicInteger ai, int toAdd) {
        int currValue;
        int newValue;
        do{
            currValue = ai.get();
            newValue = currValue + toAdd;
        } while (! ai.compareAndSet(currValue, newValue));
    }

    private static void log(String message){
        if(debugMode){
            String timeStamp = getTimeStamp(LocalDateTime.now());
            if(uploadLogs){
                uploadBuffer.append(timeStamp).append(" ").append(message).append("\n");
            }
            System.out.printf("%s %s%n",timeStamp,message);
        }
    }

    private static long min(long... nums){
        long min = Long.MAX_VALUE;
        for (long num : nums) {
            if(num < min){
                min = num;
            }
        }
        return min;
    }

    private static void executeLater(Runnable r, Box<Exception> exceptionHandler){
        executor.execute(()->{
            try{
                r.run();
            } catch (Exception e){
                exceptionHandler.set(e);
            }
        });

    }

    private static void readArgs(String[] args) {

        List<String> helpOptions = List.of("-h","-help");
        List<String> debugModeOptions = List.of("-d","-debug");
        List<String> uploadLogOptions = List.of("-ul","-uploadlog");
        List<String> uploadIntervalOptions = List.of("-ui","-uploadinterval");
        List<String> argsList = new LinkedList<>();
        argsList.addAll(helpOptions);
        argsList.addAll(debugModeOptions);
        argsList.addAll(uploadLogOptions);
        argsList.addAll(uploadIntervalOptions);
        argsList.add("-noec2");

        for (int i = 0; i < args.length; i++) {
            String arg = args[i].toLowerCase();
            String errorMessage;

            if (debugModeOptions.contains(arg)) {
                debugMode = true;
                continue;
            }
            if (uploadLogOptions.contains(arg)) {
                uploadLogs = true;
                errorMessage = "Missing upload log name\n";
                try{
                    if(argsList.contains(args[i+1])){
                        printUsageAndExit(errorMessage);
                    }
                    uploadLogName = args[i+1];
                    i++;
                    continue;
                } catch (IndexOutOfBoundsException e){
                    System.out.println();
                    printUsageAndExit(errorMessage);
                }
            }
            if (uploadIntervalOptions.contains(arg)) {
                errorMessage = "Missing upload interval\n";
                try{
                    if(argsList.contains(args[i+1])){
                        printUsageAndExit(errorMessage);
                    }
                    appendLogIntervalInSeconds = Integer.parseInt(args[i+1]);
                    i++;
                    continue;
                } catch (IndexOutOfBoundsException e){
                    printUsageAndExit(errorMessage);
                } catch (NumberFormatException e){
                    printUsageAndExit("Invalid upload interval\n");
                }
            }
            if(arg.equals("-noec2")){
                noEc2 = true;
                continue;
            }
            if (arg.equals("-h") || arg.equals("-help")) {
                printUsageAndExit("");
            }

            System.out.println();
            printUsageAndExit("Unknown argument: %s\n".formatted(arg));
        }

        if(uploadLogs && ! debugMode){
            printUsageAndExit("Upload logs flag was provided but not debug mode flag\n");
        }

        if(uploadLogs && appendLogIntervalInSeconds == 0){
            appendLogIntervalInSeconds = 60;
        }
    }
}
