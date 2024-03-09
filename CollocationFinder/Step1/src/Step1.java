import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import utils.DecadesPartitioner;


import java.io.IOException;
import java.util.*;


public class Step1 {


    private static final int TOKENS_PER_LINE = 5;
    private static final int W1_INDEX = 0;
    private static final int W2_INDEX = 1;
    private static final int DECADE_INDEX = 2;
    private static final int COUNT_OVERALL_INDEX = 3;
    private static final int DISTINCT_BOOKS_COUNT_INDEX = 4;
    private static final String BUCKET_NAME = "distributed-systems-2024-bucket-yuval-adi";
    private static boolean debug;
    private static Path inputPath;
    private static Path outputPath;
    private static String stopWordsFile;
    private static StringBuilder logBuffer;


    /**
     * emits the following format: decade -> w1,w2,count_overall
     */
    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Set<String> stopWords;
        private AmazonS3 s3;
        private boolean debug;

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            log("[Mapper] Processing line: " + value);
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            String[] tokens = new String[TOKENS_PER_LINE];
            for (int i = 0; i < TOKENS_PER_LINE; i++) {
                if (tokenizer.hasMoreTokens()) {
                    tokens[i] = tokenizer.nextToken();
                } else {
                    throw new RuntimeException("[Mapper] Expected " + TOKENS_PER_LINE + " tokens per line, but found " + (i - 1));
                }
            }

            boolean skipLine = false;
            // remove tags from words if they exist
            int index;
            if ((index = tokens[W1_INDEX].indexOf("_")) != -1){
                log("\tRemoving tag from W1: " + tokens[W1_INDEX]);
                tokens[W1_INDEX] = tokens[W1_INDEX].substring(0,index);
                log("\tW1 after removing tag: " + tokens[W1_INDEX]);
            }
            if ((index = tokens[W2_INDEX].indexOf("_")) != -1){
                log("\tRemoving tag from W2: " + tokens[W2_INDEX]);
                tokens[W2_INDEX] = tokens[W2_INDEX].substring(0,index);
                log("\tW2 after removing tag: " + tokens[W2_INDEX]);
            }
            if(tokens[W1_INDEX].isEmpty() || tokens[W2_INDEX].isEmpty()){
                log("\tW1 or W2 is empty, skipping line");
                skipLine = true;
            }

            // skip stop words
            if (stopWords.contains(tokens[W1_INDEX]) || stopWords.contains(tokens[W2_INDEX])) {
                log("\tW1 or W2 is a stop word, skipping line");
                skipLine = true;
            }

            if(! skipLine){
                String decade = tokens[DECADE_INDEX];
                Text outKey = new Text(decade.substring(0,decade.length()-1)+"0");
                Text outValue = new Text ("%s,%s,%s".formatted(tokens[W1_INDEX], tokens[W2_INDEX], tokens[COUNT_OVERALL_INDEX]));
                log("\tEmitting: " + outKey + " -> " + outValue);
                context.write(outKey, outValue);
            }
            flushLog();
        }

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            debug = Boolean.parseBoolean(context.getConfiguration().get("debug"));
            String stopWordsFile = context.getConfiguration().get("stopWordsFile");
            s3 = AmazonS3Client.builder().withRegion(Regions.US_WEST_2).build();
            log("[Mapper] Downloading stop words file from S3 from path: " + "hadoop/"+stopWordsFile);
            String stopWordsStr = downloadSmallFileFromS3(stopWordsFile);
            stopWords = new HashSet<>();
            stopWordsStr.lines().forEach(stopWords::add);
        }

        private String downloadSmallFileFromS3(String key) {

            S3Object r;
            try{
                r = s3.getObject(new GetObjectRequest(BUCKET_NAME, "hadoop/"+key));
            } catch (Exception e){
                flushLog();
                throw new RuntimeException("Failed to download file from S3", e);
            }

            // get file from response
            byte[] file = {};
            try {
                file = r.getObjectContent().readAllBytes();
            } catch (IOException e) {
                throw new RuntimeException("Failed to read file from S3", e);
            }

            return new String(file);
        }

        private void log(String s) {
            if (debug) {
                if(logBuffer == null){
                    logBuffer = new StringBuilder();
                }
                logBuffer.append(s).append("\n");
            }
        }
        private void flushLog() {
            if (debug) {
                System.out.println(logBuffer);
                logBuffer = null;
            }
        }
    }

    /**
     * emits the following format:
     *    decade,w1,w2 -> w1,w2,count_overall,bigram_count_in_decade
     */
    public static class BigramsPerDecadeReducer extends Reducer<Text, Text, Text, Text> {

        private static final int W1_VALUE_INDEX = 0;
        private static final int W2_VALUE_INDEX = 1;
        private static final int COUNT_OVERALL_VALUE_INDEX = 2;

        private boolean debug;

        @Override
        protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            debug = Boolean.parseBoolean(context.getConfiguration().get("debug"));
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            logBuffer = new StringBuilder("[Reducer] Processing key: ").append(key);
            long bigramCountInDecade = 0;
            for (Text value : values) {
                String[] valueTokens = value.toString().split(",");
                bigramCountInDecade += Long.parseLong(valueTokens[COUNT_OVERALL_VALUE_INDEX]);
            }
            log("\tBigram count in decade: " + bigramCountInDecade);
            for(Text value : values){
                String[] valueTokens = value.toString().split(",");
                String w1 = valueTokens[W1_VALUE_INDEX];
                String w2 = valueTokens[W2_VALUE_INDEX];
                String countOverall = valueTokens[COUNT_OVERALL_VALUE_INDEX];
                String valueOut = String.format("%s,%s,%s,%d", w1, w2, countOverall, bigramCountInDecade);
                log("\tEmitting: " + key + " -> " + valueOut);
                context.write(key, new Text(valueOut));
            }
            flushLog();
        }

        private void log(String s) {
            if (debug) {
                if(logBuffer == null){
                    logBuffer = new StringBuilder();
                }
                logBuffer.append(s).append("\n");
            }
        }
        private void flushLog() {
            if (debug) {
                System.out.println(logBuffer);
                logBuffer = null;
            }
        }
    }

    public static void main(String[] args){
        System.out.println("[DEBUG] STEP 1 started!");
        readArgs(args);
        Configuration conf = new Configuration();
        conf.set("stopWordsFile", stopWordsFile);
        conf.set("debug", String.valueOf(debug));
        try {
            Job job = Job.getInstance(conf, "Step1");
            job.setJarByClass(Step1.class);
            job.setMapperClass(Step1.TokenizerMapper.class);
            job.setPartitionerClass(DecadesPartitioner.class);
            job.setReducerClass(Step1.BigramsPerDecadeReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, inputPath);
            FileOutputFormat.setOutputPath(job, outputPath);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void readArgs(String[] args) {
        List<String> argsList = new LinkedList<>();
        argsList.add("-stopwordsfile");
        argsList.add("-inputurl");
        argsList.add("-outputurl");
        argsList.add("-debug");
        for (int i = 0; i < args.length; i++) {
            String arg = args[i].toLowerCase();
            String errorMessage;
            if(arg.equals("-debug")){
                debug = true;
                continue;
            }
            if (arg.equals("-stopwordsfile")) {
                errorMessage = "Missing stop words file\n";
                try{
                    if(argsList.contains(args[i+1])){
                        printErrorAndExit(errorMessage);
                    }
                    stopWordsFile = args[i+1];
                    i++;
                    continue;
                } catch (IndexOutOfBoundsException e){
                    System.out.println();
                    printErrorAndExit(errorMessage);
                }
            }
            if (arg.equals("-inputurl")) {
                errorMessage = "Missing input url\n";
                try{
                    if(argsList.contains(args[i+1])){
                        printErrorAndExit(errorMessage);
                    }
                    inputPath = new Path(args[i+1]);
                    i++;
                    continue;
                } catch (IndexOutOfBoundsException e){
                    System.out.println();
                    printErrorAndExit(errorMessage);
                }
            }
            if (arg.equals("-outputurl")) {
                errorMessage = "Missing output url\n";
                try{
                    if(argsList.contains(args[i+1])){
                        printErrorAndExit(errorMessage);
                    }
                    outputPath = new Path(args[i+1]);
                    i++;
                    continue;
                } catch (IndexOutOfBoundsException e){
                    System.out.println();
                    printErrorAndExit(errorMessage);
                }
            }
        }

        if(stopWordsFile == null){
            printErrorAndExit("Argument for stop words file not found\n");
        }
        if(inputPath == null){
            printErrorAndExit("Argument for input url not found\n");
        }
        if(outputPath == null){
            printErrorAndExit("Argument for output url not found\n");
        }
    }

    private static void printErrorAndExit(String errorMessage) {
        if(! errorMessage.equals("")) {
            System.out.println(errorMessage);
        }
        System.exit(1);
    }
}
