import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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


    private static Path inputPath;
    private static Path outputPath;
    private static String stopWordsFile;


    /**
     * emits the following format: decade -> w1,w2,count_overall
     */
    public static class TokenizerMapper extends Mapper<Text, Text, Text, Text> {

        private Set<String> stopWords;
        private AmazonS3 s3;

        @Override
        protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            String[] tokens = new String[TOKENS_PER_LINE];
            while (tokenizer.hasMoreTokens()) {
                for (int i = 0; i < TOKENS_PER_LINE; i++) {
                    if (tokenizer.hasMoreTokens()) {
                        tokens[i] = tokenizer.nextToken();
                    } else {
                        throw new RuntimeException("Expected " + TOKENS_PER_LINE + " tokens per line, but found " + (i - 1));
                    }
                }

                // remove tags from words if they exist
                int index;
                if ((index = tokens[W1_INDEX].indexOf("_")) != -1){
//                    tokens[W1_INDEX] = tokens[W1_INDEX].substring(0,index);
                    continue;
                }
                if ((index = tokens[W2_INDEX].indexOf("_")) != -1){
//                    tokens[W2_INDEX] = tokens[W2_INDEX].substring(0,index);
                    continue;
                }
//                if(tokens[W1_INDEX].isEmpty() || tokens[W2_INDEX].isEmpty()){
//                    continue;
//                }

                // skip stop words
                if (stopWords.contains(tokens[W1_INDEX]) || stopWords.contains(tokens[W2_INDEX])) {
                    continue;
                }

                String decade = tokens[DECADE_INDEX];
                Text outKey = new Text(decade.substring(0,decade.length()-1)+"0");
                Text outValue = new Text ("%s,%s,%s".formatted(tokens[W1_INDEX], tokens[W2_INDEX], tokens[COUNT_OVERALL_INDEX]));
                context.write(outKey, outValue);
            }
        }

        @Override
        protected void setup(Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String stopWordsFile = context.getConfiguration().get("stopWordsFile");
            s3 = AmazonS3Client.builder().withRegion(Regions.US_WEST_2).build();
            String stopWordsStr = downloadSmallFileFromS3(stopWordsFile);
            stopWords = new HashSet<>() {{
                addAll(Set.of(stopWordsStr.split("\n")));
            }};
        }

        private String downloadSmallFileFromS3(String key) {

            var r = s3.getObject(new GetObjectRequest(BUCKET_NAME, "hadoop/"+key));

            // get file from response
            byte[] file = {};
            try {
                file = r.getObjectContent().readAllBytes();
            } catch (IOException e) {
                throw new RuntimeException("Failed to read file from S3", e);
            }

            return new String(file);
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

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            long bigramCountInDecade = 0;
            for (Text value : values) {
                String[] valueTokens = value.toString().split(",");
                bigramCountInDecade += Long.parseLong(valueTokens[COUNT_OVERALL_VALUE_INDEX]);
            }
            for(Text value : values){
                String[] valueTokens = value.toString().split(",");
                String w1 = valueTokens[W1_VALUE_INDEX];
                String w2 = valueTokens[W2_VALUE_INDEX];
                String countOverall = valueTokens[COUNT_OVERALL_VALUE_INDEX];
                String valueOut = String.format("%s,%s,%s,%d", w1, w2, countOverall, bigramCountInDecade);
                context.write(key, new Text(valueOut));
            }
        }
    }

    public static void main(String[] args){
        System.out.println("[DEBUG] STEP 1 started!");
        readArgs(args);
        Configuration conf = new Configuration();
        conf.set("stopWordsFile", stopWordsFile);
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
        for (int i = 0; i < args.length; i++) {
            String arg = args[i].toLowerCase();
            String errorMessage;
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
