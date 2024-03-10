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
    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {

        private static final String BUCKET_NAME = "distributed-systems-2024-bucket-yuval-adi";
        private static final int W1_INDEX = 0;
        private static final int W2_INDEX = 1;
        private static final int DECADE_INDEX = 2;
        private static final int COUNT_OVERALL_INDEX = 3;
        private Set<String> stopWords;
        private AmazonS3 s3;
        private Text outKey;
        private Text outValue;

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

            outValue.set("%s,%s,%s".formatted(w1,w2,tokens[COUNT_OVERALL_INDEX]));
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
            outValue = new Text();
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
    public static class BigramsPerDecadeReducer extends Reducer<Text, Text, Text, Text> {

        private static final int KEY_DECADE_INDEX = 0;
        private static final int VALUE_W1_INDEX = 0;
        private static final int VALUE_W2_INDEX = 1;
        private static final int VALUE_INDEX = 2;
        private Text outKey;
        private Text outValue;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outKey = new Text();
            outValue = new Text();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String[] keyTokens = key.toString().split(",");
            String decade = keyTokens[KEY_DECADE_INDEX];

            var iter = values.iterator();
            if(! iter.hasNext()) return;

            String[] valueTokens = iter.next().toString().split(",");
            String w1 = valueTokens[0];
            String w2 = valueTokens[1];

            long counter = 0;
            counter += Long.parseLong(valueTokens[VALUE_INDEX]);
            while(iter.hasNext()){
                valueTokens = iter.next().toString().split(",");
                counter += Long.parseLong(valueTokens[VALUE_INDEX]);
            }

            // <decade,_,_> -- count bigrams in decade (N)
            if(keyTokens[VALUE_W1_INDEX].equals("_") && keyTokens[VALUE_W2_INDEX].equals("_")){
                // <decade,_,_> -> w1 , w2 , _ , N , _ , _
                outKey.set("%s,%s,%s".formatted(decade,"_","_"));
                outValue.set("%s,%s,%s,%s,%s,%s".formatted(w1,w2,"_",counter,"_","_"));
            }
            // <decade,w1,_> -- count c(w1) in decade
            else if(keyTokens[VALUE_W2_INDEX].equals("_")) {
                // <decade,w1,_> -> w1 , w2 , _ , _ , c(w1) , _
                outKey.set("%s,%s,%s".formatted(decade,w1,"_"));
                outValue.set("%s,%s,%s,%s,%d,%s".formatted(w1,w2,"_","_",counter,"_"));
            }
            // <decade,_,w2> -- count c(w2) in decade
            else if(keyTokens[VALUE_W1_INDEX].equals("_")){
                // <decade,_,w2> -> w1 , w2 , _ , _ , _ , c(w2)
                outKey.set("%s,%s,%s".formatted(decade,"_",w2));
                outValue.set("%s,%s,%s,%s,%s,%d".formatted(w1,w2,"_","_","_",counter));
            }
            // <decade,w1,w2> -- count c(w1,w2) in decade
            else {
                // <decade,w1,w2> -> w1 , w2 , c(w1,w2) , _ , _ , _
                outKey.set("%s,%s,%s".formatted(decade,w1,w2));
                outValue.set("%s,%s,%s,%s,%s,%s".formatted(w1,w2,counter,"_","_","_"));
            }

            context.write(outKey, outValue);
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
            job.setMapOutputValueClass(Text.class);
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
