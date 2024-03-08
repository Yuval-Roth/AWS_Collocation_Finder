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
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;

public class Step3 {

    private static Path inputPath;
    private static Path outputPath;
    private static Double minPmi;

    private static final int W1_VALUE_INDEX = 0;
    private static final int W2_VALUE_INDEX = 1;
    private static boolean debug;

    /**
     * emits the following format:
     *      decade,w1,w2 -> w1,w2,count_overall,bigram_count_in_decade,w1_count_in_decade,_,
     *      or
     *      decade,w1,w2 -> w1,w2,count_overall,bigram_count_in_decade,_,w2_count_in_decade
     */
    public static class NPMIMapper extends Mapper<Text, Text, Text, Text> {

        private static final int DECADE_KEY_INDEX = 0;

        @Override
        protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] valueTokens = value.toString().split(",");
            String[] keyTokens = key.toString().split(",");
            String w1 = valueTokens[W1_VALUE_INDEX];
            String w2 = valueTokens[W2_VALUE_INDEX];
            context.write(new Text(String.format("%s,%s,%s", keyTokens[DECADE_KEY_INDEX], w1, w2)), value);
        }
    }

    /**
     * emits the following format:
     *      decade,w1,w2 -> w1,w2,npmi
     */
    public static class NPMIReducer extends Reducer<Text, Text, Text, Text> {
        private static final int W1_VALUE_INDEX = 0;
        private static final int W2_VALUE_INDEX = 1;
        private static final int COUNT_OVERALL_VALUE_INDEX = 2;
        private static final int BIGRAM_COUNT_IN_DECADE_INDEX = 3;
        private static final int W1_COUNT_IN_DECADE_INDEX = 4;
        private static final int W2_COUNT_IN_DECADE_INDEX = 5;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] keyTokens = key.toString().split(",");
            int w1Count = 0;
            int w2Count = 0;
            for (Text value : values) {
                String[] valueTokens = value.toString().split(",");
                w1Count += valueTokens[W1_COUNT_IN_DECADE_INDEX].equals("_") ? 0 : Integer.parseInt(valueTokens[W1_COUNT_IN_DECADE_INDEX]);
                w2Count += valueTokens[W2_COUNT_IN_DECADE_INDEX].equals("_") ? 0 : Integer.parseInt(valueTokens[W2_COUNT_IN_DECADE_INDEX]);
            }
            for(Text value : values) {
                String[] valueTokens = value.toString().split(",");
                String w1 = valueTokens[W1_VALUE_INDEX];
                String w2 = valueTokens[W2_VALUE_INDEX];
                String countOverall = valueTokens[COUNT_OVERALL_VALUE_INDEX];
                String bigramCountInDecade = valueTokens[BIGRAM_COUNT_IN_DECADE_INDEX];
                double npmi = calculateNPMI(countOverall, bigramCountInDecade, w1Count, w2Count);
                if (npmi < minPmi) {
                    continue;
                }
                context.write(key, new Text("%s,%s,%s".formatted(w1,w2,String.valueOf(npmi))));
            }
        }

        private double calculateNPMI(String countOverall, String bigramCountInDecade, int w1Count, int w2Count) {
            double c_w1_w2 = Double.parseDouble(countOverall);
            double c_w1 = w1Count;
            double c_w2 = w2Count;
            double N = Double.parseDouble(bigramCountInDecade);
            double pmi = Math.log(c_w1_w2) + Math.log(N) - Math.log(c_w1) - Math.log(c_w2);
            return -1 * pmi / Math.log(c_w1_w2 / N);
        }
    }

    public static void main(String[] args){
        System.out.println("[DEBUG] STEP 3 started!");
        readArgs(args);
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf, "Step3");
            job.setJarByClass(Step3.class);
            job.setMapperClass(Step3.NPMIMapper.class);
            job.setPartitionerClass(DecadesPartitioner.class);
            job.setReducerClass(Step3.NPMIReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, inputPath);
            FileOutputFormat.setOutputPath(job, outputPath);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            handleException(e);
        }
    }

    private static void readArgs(String[] args) {
        List<String> argsList = new LinkedList<>();
        argsList.add("-minpmi");
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
            if (arg.equals("-minpmi")) {
                errorMessage = "Missing minimum pmi\n";
                try{
                    if(argsList.contains(args[i+1])){
                        printErrorAndExit(errorMessage);
                    }
                    try{
                        minPmi = Double.parseDouble(args[i+1]);
                    } catch (NumberFormatException e2){
                        System.out.println();
                        printErrorAndExit("Invalid minimum pmi\n");
                    }
                    if(minPmi < 0) {
                        System.out.println();
                        printErrorAndExit("Invalid minimum pmi\n");
                    }
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

        if(minPmi == null){
            printErrorAndExit("Argument for minimum pmi not found\n");
        }
        if(inputPath == null){
            printErrorAndExit("Argument for input url not found\n");
        }
        if(outputPath == null){
            printErrorAndExit("Argument for output url not found\n");
        }
    }

    private static void handleException(Exception e) {
        LocalDateTime now = LocalDateTime.now();
        String timeStamp = getTimeStamp(now);
        String stackTrace = stackTraceToString(e);
        System.err.println("[%s] Exception occurred:\n%s".formatted(timeStamp, stackTrace));
        throw new RuntimeException(e);
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

    private static String stackTraceToString(Exception e) {
        StringBuilder output  = new StringBuilder();
        output.append(e).append("\n");
        for (var element: e.getStackTrace()) {
            output.append("\t").append(element).append("\n");
        }
        return output.toString();
    }

    private static void printErrorAndExit(String errorMessage) {
        if(! errorMessage.equals("")) {
            System.out.println(errorMessage);
        }
        System.exit(1);
    }
}


