import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class Step3 {

    private static Path _inputPath;
    private static Path _outputPath;
    private static Double _relMinPmi;
    private static Double _minPmi;

    public static class Step3Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outKey;
        private Text outValue;
        private static final int KEY_DECADE_INDEX = 0;
        private static final int KEY_W1_INDEX = 1;
        private static final int KEY_W2_INDEX = 2;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outKey = new Text();
            outValue = new Text();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\\s+");

            String[] keyTokens = values[0].split(",");

            String decade = keyTokens[KEY_DECADE_INDEX];
            String w1 = keyTokens[KEY_W1_INDEX];
            String w2 = keyTokens[KEY_W2_INDEX];
            double npmi = Double.parseDouble(values[1]);

            outKey.set("%s,_,_,_".formatted(decade));
            outValue.set(String.valueOf(npmi));
            context.write(outKey, outValue);
            outKey.set("%s,%s,%s,%s".formatted(decade,w1,w2,npmi));
            outValue.set("");
            context.write(outKey, outValue);
        }
    }

    public static class Step3Reducer extends Reducer<Text,Text,Text,Text> {

        private static final int KEY_DECADE_INDEX = 0;
        private static final int KEY_W1_INDEX = 1;
        private static final int KEY_W2_INDEX = 2;
        private static final int KEY_NPMI_INDEX = 3;
        Text outKey;
        Text outValue;
        String currentDecade;
        private double totalNpmiInDecade;
        private double relMinPmi;
        private double minPmi;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            relMinPmi = context.getConfiguration().getDouble("relMinPmi", 0.2);
            minPmi = context.getConfiguration().getDouble("minPmi", 0.5);
            outKey = new Text();
            outValue = new Text("");
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] keyTokens = key.toString().split(",");
            String decade = keyTokens[KEY_DECADE_INDEX];

            if(currentDecade == null || !currentDecade.equals(decade)){
                currentDecade = decade;
            }

            if(keyTokens[KEY_NPMI_INDEX].equals("_")){
                for (Text value : values){
                    totalNpmiInDecade += Double.parseDouble(value.toString());
                }
            } else {
                double npmi = Double.parseDouble(keyTokens[KEY_NPMI_INDEX]);
                double relNpmi = npmi / totalNpmiInDecade;

                if(npmi < minPmi && relNpmi < relMinPmi){
                    return;
                }

                outKey.set("%s".formatted(key.toString().replaceAll(","," ")));
                context.write(outKey, outValue);
            }
        }
    }


    public static class Step3Comparator extends WritableComparator {

        private static final int DECADE_INDEX = 0;
        private static final int W1_INDEX = 1;
        private static final int W2_INDEX = 2;
        private static final int NPMI_INDEX = 3;

        Step3Comparator() {
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
            else if(aTokens[NPMI_INDEX].equals("_") && bTokens[NPMI_INDEX].equals("_")){
                return 0;
            }
            else if(aTokens[NPMI_INDEX].equals("_")){
                return -1;
            }
            else if(bTokens[NPMI_INDEX].equals("_")){
                return 1;
            }
            else if ((num = Double.valueOf(aTokens[NPMI_INDEX]).compareTo(Double.valueOf(bTokens[NPMI_INDEX]))) != 0){
                return -1 * num;
            }
            else if ((num = aTokens[W1_INDEX].compareTo(bTokens[W1_INDEX])) != 0){
                return num;
            }
            else {
                return aTokens[W2_INDEX].compareTo(bTokens[W2_INDEX]);
            }
        }
    }

    public static void main(String[] args){
        System.out.println("[DEBUG] STEP 3 started!");
        readArgs(args);
        System.out.println("[DEBUG] output path: " + _outputPath);
        System.out.println("[DEBUG] input path: " + _inputPath);
        System.out.println("[DEBUG] relMinPmi: " + _relMinPmi);
        System.out.println("[DEBUG] minPmi: " + _minPmi);
        Configuration conf = new Configuration();
        conf.set("relMinPmi", String.valueOf(_relMinPmi));
        conf.set("minPmi", String.valueOf(_minPmi));
        try {
            Job job = Job.getInstance(conf, "Step4");
            job.setJarByClass(Step3.class);
            job.setMapperClass(Step3Mapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setSortComparatorClass(Step3Comparator.class);
            FileInputFormat.addInputPath(job, _inputPath);
            FileOutputFormat.setOutputPath(job, _outputPath);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void readArgs(String[] args) {
        List<String> argsList = new LinkedList<>();
        argsList.add("-minpmi");
        argsList.add("-relminpmi");
        argsList.add("-inputurl");
        argsList.add("-outputurl");

        for (int i = 0; i < args.length; i++) {
            String arg = args[i].toLowerCase();
            String errorMessage;

            if (arg.equals("-minpmi")) {
                errorMessage = "Missing minimum pmi\n";
                try{
                    if(argsList.contains(args[i+1])){
                        printErrorAndExit(errorMessage);
                    }
                    try{
                        _minPmi = Double.parseDouble(args[i+1]);
                    } catch (NumberFormatException e2){
                        System.out.println();
                        printErrorAndExit("Invalid minimum pmi\n");
                    }
                    if(_minPmi < 0) {
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
            if (arg.equals("-relminpmi")) {
                errorMessage = "Missing relative minimum pmi\n";
                try{
                    if(argsList.contains(args[i+1])){
                        printErrorAndExit(errorMessage);
                    }
                    try{
                        _relMinPmi = Double.parseDouble(args[i+1]);
                    } catch (NumberFormatException e2){
                        System.out.println();
                        printErrorAndExit("Invalid relative minimum pmi\n");
                    }
                    if(_relMinPmi < 0) {
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

        if(_minPmi == null){
            printErrorAndExit("Argument for minimum pmi not found\n");
        }
        if(_relMinPmi == null){
            printErrorAndExit("Argument for relative minimum pmi not found\n");
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


