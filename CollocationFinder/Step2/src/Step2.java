import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class Step2 {

    private static Path _inputPath;
    private static Path _outputPath;

    public static class Step2Mapper extends Mapper<LongWritable, Text, Text, Text> {

        // <KEY INDEXES>
        private static final int DECADE_KEY_INDEX = 0;
        private static final int W1_KEY_INDEX = 1;
        private static final int W2_KEY_INDEX = 2;
        // </KEY INDEXES>

        private Text outKey;
        private Text outValue;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outKey = new Text();
            outValue = new Text();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\\s+");
            String[] keyTokens = values[0].split(",");

            String decade = keyTokens[DECADE_KEY_INDEX];
            String w1 = keyTokens[W1_KEY_INDEX];
            String w2 = keyTokens[W2_KEY_INDEX];


            outValue.set(values[1]);
            outKey.set("%s,%s,_".formatted(decade,w2));
            context.write(outKey, outValue);
            outKey.set("%s,%s,%s".formatted(decade,w2,w1));
            context.write(outKey, outValue);
        }
    }

    public static class Step2Reducer extends Reducer<Text, Text, Text, DoubleWritable> {

        private static final int KEY_DECADE_INDEX = 0;
        private static final int KEY_W1_INDEX = 1;
        private static final int KEY_W2_INDEX = 2;
        private static final int VALUE_C_W1_W2_INDEX = 0;
        private static final int VALUE_N_INDEX = 1;
        private static final int VALUE_C_W1_INDEX = 2;

        private long c_w2;
        String currentDecade;
        String currentW1;
        Text outKey;
        DoubleWritable outValue;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outKey = new Text();
            outValue = new DoubleWritable();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String[] keyTokens = key.toString().split(",");
            String decade = keyTokens[KEY_DECADE_INDEX];
            String w1 = keyTokens[KEY_W1_INDEX];
            String w2 = keyTokens[KEY_W2_INDEX];

            if(currentDecade == null || !currentDecade.equals(decade)){
                currentDecade = decade;
                currentW1 = null;
            }

            if(currentW1 == null || !currentW1.equals(w1)){
                currentW1 = w1;
                c_w2 = 0;
            }

            long counter = 0;

            // count c_w2 in decade
            if(w2.equals("_")) {
                for (Text value : values) {
                    String[] valueTokens = value.toString().split(",");
                    counter += Long.parseLong(valueTokens[VALUE_C_W1_W2_INDEX]);
                }
                c_w2 = counter;
            }
            // emit c_w1_w2 in decade
            else {
                Iterator<Text> valuesIterator = values.iterator();
                Text value = valuesIterator.next();
                outKey.set("%s,%s,%s".formatted(decade,w2,w1));

                String[] valueTokens = value.toString().split(",");
                double c_w1_w2 = Double.parseDouble(valueTokens[VALUE_C_W1_W2_INDEX]);
                double N = Double.parseDouble(valueTokens[VALUE_N_INDEX]);
                double c_w1 = Double.parseDouble(valueTokens[VALUE_C_W1_INDEX]);

                if(Math.log(c_w1_w2/N) == 0) return; // division by zero

                double npmi = calculateNPMI(c_w1_w2, N, c_w1, c_w2);

                outValue.set(npmi);
                context.write(outKey, outValue);

                // should be only one value
                if(valuesIterator.hasNext()){
                    throw new RuntimeException("More than one value for key: %s".formatted(key));
                }
            }
        }

        private double calculateNPMI(double c_w1_w2, double N, double c_w1, double c_w2) {
            double pmi = Math.log(c_w1_w2) + Math.log(N) - Math.log(c_w1) - Math.log(c_w2);
            return -1 * pmi / Math.log(c_w1_w2 / N);
        }
    }

    public static class Step2Partitioner extends Partitioner<Text, Text> {
        public int getPartition(Text key, Text value, int numPartitions) {
            String[] keyTokens = key.toString().split(",");
            return Integer.parseInt(String.valueOf(keyTokens[0].charAt(2))) % numPartitions;
        }
    }

    public static class Step2Combiner extends Reducer<Text, Text, Text, Text>{
        private static final int KEY_W2_INDEX = 2;
        private static final int VALUE_C_W1_W2_INDEX = 0;
        private static final int VALUE_N_INDEX = 1;
        private static final int VALUE_C_W1_INDEX = 2;

        Text outValue;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outValue = new Text();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] keyTokens = key.toString().split(",");
            String w2 = keyTokens[KEY_W2_INDEX];

            long counter = 0;

            // count c_w2 in decade
            Iterator<Text> valuesIterator = values.iterator();
            if(w2.equals("_")) {
                String[] valueTokens = valuesIterator.next().toString().split(",");
                counter += Long.parseLong(valueTokens[VALUE_C_W1_W2_INDEX]);
                while(valuesIterator.hasNext()){
                    valueTokens = valuesIterator.next().toString().split(",");
                    counter += Long.parseLong(valueTokens[VALUE_C_W1_W2_INDEX]);
                }
                outValue.set("%d,%s,%s".formatted(counter,valueTokens[VALUE_N_INDEX],valueTokens[VALUE_C_W1_INDEX]));
                context.write(key, outValue);
            }
            // emit c_w1_w2 in decade
            else {
                Text value = valuesIterator.next();
                context.write(key,value);

                // should be only one value
                if(valuesIterator.hasNext()){
                    throw new RuntimeException("More than one value for key: %s".formatted(key));
                }
            }
        }
    }

    public static class Step2Comparator extends WritableComparator {

        private static final int DECADE_INDEX = 0;
        private static final int W1_INDEX = 1;
        private static final int W2_INDEX = 2;

        Step2Comparator() {
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

    public static void main(String[] args){
        System.out.println("[DEBUG] STEP 2 started!");
        readArgs(args);
        System.out.println("[DEBUG] output path: " + _outputPath);
        System.out.println("[DEBUG] input path: " + _inputPath);
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf, "Step2");
            job.setJarByClass(Step2.class);
            job.setMapperClass(Step2Mapper.class);
            job.setSortComparatorClass(Step2Comparator.class);
            job.setPartitionerClass(Step2Partitioner.class);
            job.setReducerClass(Step2Reducer.class);
            job.setCombinerClass(Step2Combiner.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
            FileInputFormat.addInputPath(job, _inputPath);
            FileOutputFormat.setOutputPath(job, _outputPath);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void readArgs(String[] args) {
        List<String> argsList = new LinkedList<>();
        argsList.add("-inputurl");
        argsList.add("-outputurl");

        for (int i = 0; i < args.length; i++) {
            String arg = args[i].toLowerCase();
            String errorMessage;

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


