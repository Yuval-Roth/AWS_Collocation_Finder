import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;


public class Step1 {

    enum Language {
        english,
        hebrew
    }

    private static Path _inputPath;
    private static Path _outputPath;
    private static String _stopWordsFile;
    private static boolean _compressed;
    private static Double _corpusPercentage;
    private static Language _language;

    public static class Step1Mapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private static final String BUCKET_NAME = "distributed-systems-2024-bucket-yuval-adi";
        private static final int W1_INDEX = 0;
        private static final int W2_INDEX = 1;
        private static final int DECADE_INDEX = 2;
        private static final int COUNT_OVERALL_INDEX = 3;
        private Set<String> stopWords;
        private AmazonS3 s3;
        private LongWritable outValue;
        private Text outKey;
        private double corpusPercentage;
        private Language language;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(Math.random() > corpusPercentage){
                return;
            }
            String[] tokens = value.toString().split("\\s+");

            if(language == Language.english){
                tokens[W1_INDEX] = tokens[W1_INDEX].toLowerCase();
                tokens[W2_INDEX] = tokens[W2_INDEX].toLowerCase();

                if(!tokens[W1_INDEX].matches("[a-z]*") || !tokens[W2_INDEX].matches("[a-z]*")){
                    return;
                }
            } else {
                if(!tokens[W1_INDEX].matches("[א-ת]*") || !tokens[W2_INDEX].matches("[א-ת]*")){
                    return;
                }
            }

            // skip stop words
            if (stopWords.contains(tokens[W1_INDEX]) || stopWords.contains(tokens[W2_INDEX])) {return;}

            String decade = tokens[DECADE_INDEX];
            decade = decade.substring(0, decade.length() - 1) + "0";
            String w1 = tokens[W1_INDEX];
            String w2 = tokens[W2_INDEX];

            outValue.set(Long.parseLong(tokens[COUNT_OVERALL_INDEX]));
            outKey.set("%s,_,_".formatted(decade));
            context.write(outKey, outValue);
            outKey.set("%s,%s,_".formatted(decade,w1));
            context.write(outKey, outValue);
            outKey.set("%s,%s,%s".formatted(decade,w1,w2));
            context.write(outKey, outValue);
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outKey = new Text();
            outValue = new LongWritable();

            // get configuration
            Configuration conf = context.getConfiguration();
            corpusPercentage = conf.getDouble("corpusPercentage", 1.0);
            language = Language.valueOf(conf.get("language"));
            String stopWordsFile = conf.get("stopWordsFile");

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

    public static class Step1Comparator extends WritableComparator {

        private static final int DECADE_INDEX = 0;
        private static final int W1_INDEX = 1;
        private static final int W2_INDEX = 2;

        Step1Comparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            String[] aTokens = a.toString().split(",");
            String[] bTokens = b.toString().split(",");
            int num;
            if((num = aTokens[DECADE_INDEX].compareTo(bTokens[DECADE_INDEX])) != 0){
                return num;
            }
            else if ((num = aTokens[W1_INDEX].compareTo(bTokens[W1_INDEX])) != 0){
                return num;
            }
            else {
                return aTokens[W2_INDEX].compareTo(bTokens[W2_INDEX]);
            }
        }
    }

    /**
     * emits the following format:
     *    decade,w1,w2 -> w1,w2,count_overall,bigram_count_in_decade
     */
    public static class Step1Reducer extends Reducer<Text, LongWritable, Text, Text> {

        private static final int KEY_DECADE_INDEX = 0;
        private static final int KEY_W1_INDEX = 1;
        private static final int KEY_W2_INDEX = 2;
        Text outKey;
        Text outValue;
        String currentW1;
        long c_w1;
        String currentDecade;
        long N;

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

            if(currentDecade == null || !currentDecade.equals(decade)){
                currentDecade = decade;
                currentW1 = null;
                N = 0;
            }
            if(currentW1 == null || !currentW1.equals(w1)){
                currentW1 = w1;
                c_w1 = 0;
            }

            long counter = 0;

            // count N in decade
            if(w1.equals("_") && w2.equals("_")) {
                for (LongWritable value : values) {
                    counter += value.get();
                }
                N = counter;
            }
            // count c_w1 in decade
            else if(w2.equals("_")) {
                for (LongWritable value : values) {
                    counter += value.get();
                }
                c_w1 = counter;
            }
            // count c_w1_w2 in decade
            else {
                for (LongWritable value : values) {
                    counter += value.get();
                }
                outKey.set("%s,%s,%s".formatted(decade,w1,w2));
                outValue.set("%s,%s,%s".formatted(counter,N,c_w1));
                context.write(outKey, outValue);
            }
        }
    }

    public static class Step1Combiner extends Reducer<Text,LongWritable,Text,LongWritable>{
        LongWritable outValue;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outValue = new LongWritable();
        }

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long counter = 0;
            for (LongWritable value : values) {
                counter += value.get();
            }
            outValue.set(counter);
            context.write(key, outValue);
        }
    }

    public static class Step1Partitioner extends Partitioner<Text, LongWritable> {
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            String[] keyTokens = key.toString().split(",");
            return Integer.parseInt(keyTokens[0]) % numPartitions;
        }
    }

    public static void main(String[] args){
        System.out.println("[DEBUG] STEP 1 started!");
        readArgs(args);
        System.out.println("[DEBUG] output path: " + _outputPath);
        System.out.println("[DEBUG] input path: " + _inputPath);
        System.out.println("[DEBUG] stop words file: " + _stopWordsFile);
        System.out.println("[DEBUG] language: " + _language);
        System.out.println("[DEBUG] corpus percentage: " + _corpusPercentage);
        System.out.printf("[DEBUG] compressed: "+ _compressed );
        Configuration conf = new Configuration();
        conf.set("stopWordsFile", _stopWordsFile);
        conf.set("language", _language.toString());
        if(_corpusPercentage != null) {
            conf.setDouble("corpusPercentage", _corpusPercentage);
        }
        try {
            Job job = Job.getInstance(conf, "Step1");
            job.setJarByClass(Step1.class);
            job.setMapperClass(Step1Mapper.class);
            job.setPartitionerClass(Step1Partitioner.class);
            job.setSortComparatorClass(Step1Comparator.class);
            job.setReducerClass(Step1Reducer.class);
            job.setCombinerClass(Step1Combiner.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setNumReduceTasks(11);
            if(_compressed) {
                job.setInputFormatClass(SequenceFileInputFormat.class);
            }
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
        argsList.add("-corpuspercentage");
        argsList.add("-compressed");
        argsList.add("-language");
        for (int i = 0; i < args.length; i++) {
            String arg = args[i].toLowerCase();
            String errorMessage;
            if (arg.equals("-language")) {
                errorMessage = "Bad language argument\n";
                try{
                    if(argsList.contains(args[i+1])){
                        printErrorAndExit(errorMessage);
                    }
                    _language = Language.valueOf(args[i+1]);
                    i++;
                    continue;
                } catch (IllegalArgumentException e){
                    System.out.println();
                    printErrorAndExit("Language can be either 'hebrew' or 'english'\n");
                }
            }
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
            if (arg.equals("-corpuspercentage")) {
                errorMessage = "Missing corpus percentage\n";
                try{
                    if(argsList.contains(args[i+1])){
                        printErrorAndExit(errorMessage);
                    }
                    _corpusPercentage = Double.parseDouble(args[i+1]);
                    i++;
                    continue;
                } catch (IndexOutOfBoundsException e){
                    System.out.println();
                    printErrorAndExit(errorMessage);
                }
            }
            if (arg.equals("-compressed")) {
                _compressed = true;
                continue;
            }
        }
        if(_language == null){
            printErrorAndExit("Argument for language not found\n");
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
