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
import java.util.LinkedList;
import java.util.List;

public class Step2 {

    private static Path inputPath;
    private static Path outputPath;

    private static final int W1_INDEX = 0;
    private static final int W2_INDEX = 1;
    private static final int COUNT_OVERALL_INDEX = 3;
    private static final int BIGRAM_COUNT_IN_DECADE = 4;
    private static boolean debug;

    /**
     * emits the following format:
     *     decade,w1,_ -> w1,w2,count_overall,bigram_count_in_decade
     *     decade,_,w2 -> w1,w2,count_overall,bigram_count_in_decade
     */
    public static class C_W_Mapper extends Mapper<Text, Text, Text, Text> {

        private String[] tokens;

        @Override
        protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

            log("Processing line: " + value);
            String[] valueTokens = value.toString().split(",");
            String w1 = valueTokens[W1_INDEX];
            String w2 = valueTokens[W2_INDEX];
            String countOverall = valueTokens[COUNT_OVERALL_INDEX];
            String bigramCountInDecade = valueTokens[BIGRAM_COUNT_IN_DECADE];
            String valueOut = String.format("%s,%s,%s,%s", w1, w2, countOverall, bigramCountInDecade);
            context.write(new Text(String.format("%s,%s,_", key, w1)), new Text(valueOut));
            context.write(new Text(String.format("%s,_,%s", key, w2)), new Text(valueOut));

        }
    }

    /**
     * emits the following format:
     *      decade,w1,_ -> w1,w2,count_overall,bigram_count_in_decade,w1_count_in_decade,_
     *      or
     *      decade,_,w2 -> w1,w2,count_overall,bigram_count_in_decade,_,w2_count_in_decade
     */
    public static class C_W_Reducer extends Reducer<Text, Text, Text, Text> {

        private static final int W1_KEY_INDEX = 1;
        private static final int W2_KEY_INDEX = 2;
        private static final int W1_VALUE_INDEX = 0;
        private static final int W2_VALUE_INDEX = 1;
        private static final int COUNT_OVERALL_VALUE_INDEX = 2;
        private static final int BIGRAM_COUNT_IN_DECADE_INDEX = 3;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] keyTokens = key.toString().split(",");
            int wCount = 0;
            for (Text value : values) {
                String[] valueTokens = value.toString().split(",");
                wCount += Integer.parseInt(valueTokens[COUNT_OVERALL_VALUE_INDEX]);
            }
            for(Text value : values) {
                String[] valueTokens = value.toString().split(",");
                String w1 = valueTokens[W1_VALUE_INDEX];
                String w2 = valueTokens[W2_VALUE_INDEX];
                String countOverall = valueTokens[COUNT_OVERALL_VALUE_INDEX];
                String bigramCountInDecade = valueTokens[BIGRAM_COUNT_IN_DECADE_INDEX];
                String valueOut = String.format("%s,%s,%s,%s,%s,%s",
                        w1, w2, countOverall, bigramCountInDecade,
                        keyTokens[W1_KEY_INDEX].equals("_") ? "_" : wCount,
                        keyTokens[W2_KEY_INDEX].equals("_") ? "_" : wCount);
                context.write(key, new Text(valueOut));
            }
        }
    }
    public static void main(String[] args){
        System.out.println("[DEBUG] STEP 2 started!");
        readArgs(args);
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf, "Step2");
            job.setJarByClass(Step2.class);
            job.setMapperClass(Step2.C_W_Mapper.class);
            job.setPartitionerClass(DecadesPartitioner.class);
            job.setReducerClass(Step2.C_W_Reducer.class);
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

    private static void log(String s) {
        if (debug) {
            System.out.println(s);
        }
    }

    private static void readArgs(String[] args) {
        List<String> argsList = new LinkedList<>();
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


