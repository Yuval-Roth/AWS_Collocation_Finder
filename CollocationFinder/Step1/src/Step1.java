import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
    private static Path _inputPath;
    private static Path _outputPath;
    private static String _stopWordsFile;

    /**
     * emits the following format:
     *
     *  decade,w1,w2 -> count_overall
     *  decade,w1,_ -> count_overall
     *  decade,_,w2 -> count_overall
     *  decade,_,_ -> count_overall
     */
    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private static final String BUCKET_NAME = "distributed-systems-2024-bucket-yuval-adi";
        private static final int W1_INDEX = 0;
        private static final int W2_INDEX = 1;
        private static final int DECADE_INDEX = 2;
        private static final int COUNT_OVERALL_INDEX = 3;
        private Set<String> stopWords;
        private AmazonS3 s3;
        private Text outKey;
        private LongWritable outValue;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\s+");

            // remove tags from words if they exist
            int index;
            if ((index = tokens[W1_INDEX].indexOf("_")) != -1){
                tokens[W1_INDEX] = tokens[W1_INDEX].substring(0,index);
            }
            if ((index = tokens[W2_INDEX].indexOf("_")) != -1){
                tokens[W2_INDEX] = tokens[W2_INDEX].substring(0,index);
            }
            if(tokens[W1_INDEX].isEmpty() || tokens[W2_INDEX].isEmpty()){
                return;
            }

            // skip stop words
            if (stopWords.contains(tokens[W1_INDEX]) || stopWords.contains(tokens[W2_INDEX])) {
                return;
            }

            String decade = tokens[DECADE_INDEX];
            decade = decade.substring(0, decade.length() - 1) + "0";
            String w1 = tokens[W1_INDEX];
            String w2 = tokens[W2_INDEX];

            outValue.set(Long.parseLong(tokens[COUNT_OVERALL_INDEX]));
            outKey.set("%s,%s,%s".formatted(decade,w1,w2));
            context.write(outKey, outValue);
            outKey.set("%s,%s,_".formatted(decade,w1));
            context.write(outKey, outValue);
            outKey.set("%s,_,%s".formatted(decade,w2));
            context.write(outKey, outValue);
            outKey.set("%s,_,_".formatted(decade));
            context.write(outKey, outValue);
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outKey = new Text();
            outValue = new LongWritable();
            String stopWordsFile = context.getConfiguration().get("stopWordsFile");
            s3 = AmazonS3Client.builder().withRegion(Regions.US_WEST_2).build();
            String stopWordsStr = downloadSmallFileFromS3(stopWordsFile);
            stopWords = new HashSet<>();
            stopWordsStr.lines().forEach(stopWords::add);
        }

        private String downloadSmallFileFromS3(String key) {

            S3Object r = s3.getObject(new GetObjectRequest(BUCKET_NAME, "hadoop/" + key));

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
    public static class BigramsPerDecadeReducer extends Reducer<Text, LongWritable, Text, Text> {

        private static final int KEY_DECADE_INDEX = 0;
        private static final int KEY_W1_INDEX = 1;
        private static final int KEY_W2_INDEX = 2;

        private static final int VALUE_C1_C2_COUNT_INDEX = 0;
        private static final int VALUE_BIGRAM_COUNT_IN_DECADE_INDEX = 1;
        private static final int VALUE_C1_COUNT_INDEX = 2;
        private static final int VALUE_C2_COUNT_INDEX = 3;
        private Text outKey;
        private Text outValue;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outKey = new Text();
            outValue = new Text();
        }

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            String[] keyTokens = key.toString().split(",");
            String decade = keyTokens[KEY_DECADE_INDEX];
            String w1 = keyTokens[KEY_W1_INDEX];
            String w2 = keyTokens[KEY_W2_INDEX];

            // <decade,_,_> -- count bigrams in decade (N)
            if(keyTokens[KEY_W1_INDEX].equals("_") && keyTokens[KEY_W2_INDEX].equals("_")){
                long bigramCountInDecade = 0;
                for (LongWritable value : values) {
                    bigramCountInDecade += value.get();
                }
                outKey.set("%s,%s,%s".formatted(decade,"_","_"));
                outValue.set("%s,%s,%s,%s,%s,%s".formatted(w1,w2,"_",bigramCountInDecade,"_","_"));
                context.write(outKey, outValue);
            }
            // <decade,w1,_> -- count c(w1) in decade
            else if(keyTokens[KEY_W2_INDEX].equals("_")) {
                long w1count = 0;
                for (LongWritable value : values) {
                    w1count += value.get();
                }
                outKey.set("%s,%s,%s".formatted(decade,w1,"_"));
                outValue.set("%s,%s,%s,%s,%d,%s".formatted(w1,w2,"_","_",w1count,"_"));
                context.write(outKey, outValue);
            }
            // <decade,_,w2> -- count c(w2) in decade
            else if(keyTokens[KEY_W1_INDEX].equals("_")){
                long w2count = 0;
                for (LongWritable value : values) {
                    w2count += value.get();
                }
                outKey.set("%s,%s,%s".formatted(decade,"_",w2));
                outValue.set("%s,%s,%s,%s,%s,%d".formatted(w1,w2,"_","_","_",w2count));
                context.write(outKey, outValue);
            }
            // <decade,w1,w2> -- count c(w1,w2) in decade
            else {
                long c_w1_w2 = 0;
                for (LongWritable value : values) {
                    c_w1_w2 += value.get();
                }
                outKey.set("%s,%s,%s".formatted(decade,w1,w2));
                outValue.set("%s,%s,%s,%s,%s,%s".formatted(w1,w2,c_w1_w2,"_","_","_"));
                context.write(outKey, outValue);
            }
        }
    }

    public static void main(String[] args){
        System.out.println("[DEBUG] STEP 1 started!");
        readArgs(args);
        System.out.println("[DEBUG] output path: " + _outputPath);
        System.out.println("[DEBUG] input path: " + _inputPath);
        System.out.println("[DEBUG] stop words file: " + _stopWordsFile);
        Configuration conf = new Configuration();
        conf.set("stopWordsFile", _stopWordsFile);
        try {
            Job job = Job.getInstance(conf, "Step1");
            job.setJarByClass(Step1.class);
            job.setMapperClass(TokenizerMapper.class);
            job.setPartitionerClass(DecadesPartitioner.class);
            job.setReducerClass(BigramsPerDecadeReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, _inputPath);
            FileOutputFormat.setOutputPath(job, _outputPath);
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
                    _stopWordsFile = args[i+1];
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
                    _inputPath = new Path(args[i+1]);
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
                    _outputPath = new Path(args[i+1]);
                    i++;
                    continue;
                } catch (IndexOutOfBoundsException e){
                    System.out.println();
                    printErrorAndExit(errorMessage);
                }
            }
        }

        if(_stopWordsFile == null){
            printErrorAndExit("Argument for stop words file not found\n");
        }
        if(_inputPath == null){
            printErrorAndExit("Argument for input url not found\n");
        }
        if(_outputPath == null){
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
