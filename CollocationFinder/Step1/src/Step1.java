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
import java.io.OutputStream;
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
        private LongWritable outValue;
        private Text outKeyBoth;
        private Text outKeyW1;
        private Text outKeyW2;
        private Text outKeyNone;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\s+");

            boolean skipLine = false;
            // remove tags from words if they exist
            int index;
            if ((index = tokens[W1_INDEX].indexOf("_")) != -1){
                tokens[W1_INDEX] = tokens[W1_INDEX].substring(0,index);
            }
            if ((index = tokens[W2_INDEX].indexOf("_")) != -1){
                tokens[W2_INDEX] = tokens[W2_INDEX].substring(0,index);
            }
            if(tokens[W1_INDEX].isEmpty() || tokens[W2_INDEX].isEmpty()){
                skipLine = true;
            }

            // skip stop words
            if (stopWords.contains(tokens[W1_INDEX]) || stopWords.contains(tokens[W2_INDEX])) {
                skipLine = true;
            }

            if(! skipLine){
                String decade = tokens[DECADE_INDEX];
                decade = decade.substring(0, decade.length() - 1) + "0";
                String w1 = tokens[W1_INDEX];
                String w2 = tokens[W2_INDEX];

                outKeyBoth.set("%s,%s,%s".formatted(decade,w1,w2));
                outKeyW1.set("%s,%s,_".formatted(decade,w1));
                outKeyW2.set("%s,_,%s".formatted(decade,w2));
                outKeyNone.set("%s,_,_".formatted(decade));
                outValue.set(Long.parseLong(tokens[COUNT_OVERALL_INDEX]));
                context.write(outKeyBoth, outValue);
                context.write(outKeyW1, outValue);
                context.write(outKeyW2, outValue);
                context.write(outKeyNone, outValue);
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outKeyBoth = new Text();
            outKeyW1 = new Text();
            outKeyW2 = new Text();
            outKeyNone = new Text();
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
    public static class BigramsPerDecadeReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        private static final int KEY_DECADE_INDEX = 0;
        private static final int KEY_W1_INDEX = 1;
        private static final int KEY_W2_INDEX = 2;
        FileSystem fs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            fs = FileSystem.get(context.getConfiguration());
        }

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            Path folderPath = new Path("hdfs:///step1/");
            fs.mkdirs(folderPath);
            String[] keyTokens = key.toString().split(",");
            String decade = keyTokens[KEY_DECADE_INDEX];
            String w1 = keyTokens[KEY_W1_INDEX];
            String w2 = keyTokens[KEY_W2_INDEX];

            long counter = 0;
            for (LongWritable value : values) {
                counter += value.get();
            }

            // <decade,w1,w2> -- count C(w1,w2) in decade
            if(! (w1.equals("_") || w2.equals("_"))){
                context.write(key, new LongWritable(counter));
            } else{
                Path filePath;

                // <decade,_,_> -- count bigrams in decade (N)
                if(keyTokens[KEY_W1_INDEX].equals("_") && keyTokens[KEY_W2_INDEX].equals("_")){
                    //"hdfs:///jobs1/1990-_-_"
                    filePath = new Path(folderPath, "%s-_-_".formatted(decade));
                }
                // <decade,w1,_> -- count c(w1) in decade
                else if(keyTokens[KEY_W2_INDEX].equals("_")) {
                    //"hdfs:///jobs1/1990-w1-_"
                    filePath = new Path(folderPath, "%s-%s-_".formatted(decade,w1));
                }
                // <decade,_,w2> -- count c(w2) in decade
                else {
                    //"hdfs:///jobs1/1990-_-w2"
                    filePath = new Path(folderPath, "%s-_-%s".formatted(decade,w2));
                }

                OutputStream s = fs.create(filePath);
                s.write(String.valueOf(counter).getBytes());
                s.close();
            }
        }
    }

    public static void main(String[] args){
        System.out.println("[DEBUG] STEP 1 started!");
        readArgs(args);
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
            job.setOutputValueClass(LongWritable.class);
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
