import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import utils.DecadesPartitioner;
import utils.DescendingComparator;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;

public class Step4 {

    private static Path inputPath;
    private static Path outputPath;
    private static Double relMinPmi;

    /**
     * emits the following format:
     *      decade -> w1,w2,npmi
     */
    public static class RelNPMIMapper extends Mapper<Text, Text, Text, Text> {

        private static final int DECADE_KEY_INDEX = 0;

        @Override
        protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] keyTokens = key.toString().split(",");
            context.write(new Text(keyTokens[DECADE_KEY_INDEX]),value);
        }
    }

    /**
     * emits the following format:
     *      decade,npmi,w1,w2 -> ""
     */
    public static class RelNPMIReducer extends Reducer<Text, Text, Text, Text> {

        private static final int W1_VALUE_INDEX = 0;
        private static final int W2_VALUE_INDEX = 1;
        private static final int NPMI_VALUE_INDEX = 2;
        private double relMinPmi;

        @Override
        protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            relMinPmi = Double.parseDouble(context.getConfiguration().get("relMinPmi"));
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String decade = key.toString();
            double npmiTotal = 0;
            for(Text value : values){
                String[] valueTokens = value.toString().split(",");
                npmiTotal += Double.parseDouble(valueTokens[NPMI_VALUE_INDEX]);
            }
            for (Text value : values) {
                String[] valueTokens = value.toString().split(",");
                String w1 = valueTokens[W1_VALUE_INDEX];
                String w2 = valueTokens[W2_VALUE_INDEX];
                double npmi = Double.parseDouble(valueTokens[NPMI_VALUE_INDEX]);
                double relNpmi = npmi / npmiTotal;
                if (relNpmi < relMinPmi) {
                    continue;
                }
                context.write(new Text("%s,%f,%s,%s".formatted(decade,npmi,w1,w2)), new Text(""));
            }
        }
    }

    public static void main(String[] args){
        System.out.println("[DEBUG] STEP 4 started!");
        readArgs(args);
        Configuration conf = new Configuration();
        conf.set("relMinPmi", String.valueOf(relMinPmi));
        try {
            Job job = Job.getInstance(conf, "Step4");
            job.setJarByClass(Step4.class);
            job.setMapperClass(Step4.RelNPMIMapper.class);
            job.setPartitionerClass(DecadesPartitioner.class);
            job.setReducerClass(Step4.RelNPMIReducer.class);
            job.setSortComparatorClass(DescendingComparator.class);
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
        argsList.add("-relminpmi");
        argsList.add("-inputurl");
        argsList.add("-outputurl");
        for (int i = 0; i < args.length; i++) {
            String arg = args[i].toLowerCase();
            String errorMessage;
            if (arg.equals("-relminpmi")) {
                errorMessage = "Missing relative minimum pmi\n";
                try{
                    if(argsList.contains(args[i+1])){
                        printErrorAndExit(errorMessage);
                    }
                    try{
                        relMinPmi = Double.parseDouble(args[i+1]);
                    } catch (NumberFormatException e2){
                        System.out.println();
                        printErrorAndExit("Invalid relative minimum pmi\n");
                    }
                    if(relMinPmi < 0) {
                        System.out.println();
                        printErrorAndExit("Invalid relative minimum pmi\n");
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

        if(relMinPmi == null){
            printErrorAndExit("Argument for relative minimum pmi not found\n");
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


