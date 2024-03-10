import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import utils.DecadesPartitioner;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class Step2 {

    private static Path _inputPath;
    private static Path _outputPath;
    private static Double _minPmi;

    public static class C_W_Mapper extends Mapper<LongWritable, Text, Text, Text> {


        // <KEY INDEXES>
        private static final int DECADE_KEY_INDEX = 0;
        private static final int W1_KEY_INDEX = 1;
        private static final int W2_KEY_INDEX = 2;
        // </KEY INDEXES>

        FileSystem fs;
        private Text outKey;
        private Text outValue;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outKey = new Text();
            outValue = new Text();
            fs = FileSystem.get(context.getConfiguration());
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            Path folderPath = new Path("hdfs:///step1/");
            String[] values = value.toString().split("\\s+");

            String[] keyTokens = values[0].split(",");
            String decade = keyTokens[DECADE_KEY_INDEX];
            String w1 = keyTokens[W1_KEY_INDEX];
            String w2 = keyTokens[W2_KEY_INDEX];
            String countOverall = values[1];
            String bigramCountInDecade;
            String w1CountInDecade;
            String w2CountInDecade;
            Path bigramCountPath = new Path(folderPath, "%s-_-_".formatted(decade));
            Path w1CountPath = new Path(folderPath, "%s-%s-_".formatted(decade, w1));
            Path w2CountPath = new Path(folderPath, "%s-_-%s".formatted(decade, w2));
            try(BufferedInputStream reader = new BufferedInputStream(fs.open(bigramCountPath))){
                bigramCountInDecade = new String(reader.readAllBytes());
            }
            try(BufferedInputStream reader = new BufferedInputStream(fs.open(w1CountPath))){
                w1CountInDecade = new String(reader.readAllBytes());
            }
            try(BufferedInputStream reader = new BufferedInputStream(fs.open(w2CountPath))){
                w2CountInDecade = new String(reader.readAllBytes());
            }
            outKey.set(values[0]);
            outValue.set("%s,%s,%s,%s".formatted(countOverall,
                    bigramCountInDecade,w1CountInDecade,w2CountInDecade));
            context.write(outKey, outValue);
        }
    }

    public static class C_W_Reducer extends Reducer<Text, Text, Text, DoubleWritable> {

        // <KEY INDEXES>
        private static final int DECADE_KEY_INDEX = 0;
        // </KEY INDEXES>

        // <VALUE INDEXES>
        private static final int COUNT_OVERALL_VALUE_INDEX = 0;
        private static final int BIGRAM_COUNT_IN_DECADE_INDEX = 1;
        private static final int W1_COUNT_IN_DECADE_INDEX = 2;
        private static final int W2_COUNT_IN_DECADE_INDEX = 3;
        // </VALUE INDEXES>

        FileSystem fs;
        DoubleWritable outValue;
        private double minPmi;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outValue = new DoubleWritable();
            fs = FileSystem.get(context.getConfiguration());
            minPmi = Double.parseDouble(context.getConfiguration().get("minPmi"));
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value : values){
                Double[] valueDoubles = Arrays.stream(value.toString().split(","))
                        .map(Double::parseDouble)
                        .toArray(Double[]::new);
                double npmi = calculateNPMI(
                        valueDoubles[BIGRAM_COUNT_IN_DECADE_INDEX],
                        valueDoubles[COUNT_OVERALL_VALUE_INDEX],
                        valueDoubles[W1_COUNT_IN_DECADE_INDEX],
                        valueDoubles[W2_COUNT_IN_DECADE_INDEX]);

                if(npmi < minPmi){
                    continue;
                }
                outValue.set(npmi);
                context.write(key, outValue);;
            }
        }

        private double calculateNPMI(double c_w1_w2, double N, double c_w1, double c_w2) {
            double pmi = Math.log(c_w1_w2) + Math.log(N) - Math.log(c_w1) - Math.log(c_w2);
            return -1 * pmi / Math.log(c_w1_w2 / N);
        }

    }
    public static void main(String[] args){
        System.out.println("[DEBUG] STEP 2 started!");
        readArgs(args);
        Configuration conf = new Configuration();
        conf.set("minPmi", String.valueOf(_minPmi));
        try {
            Job job = Job.getInstance(conf, "Step2");
            job.setJarByClass(Step2.class);
            job.setMapperClass(C_W_Mapper.class);
            job.setPartitionerClass(DecadesPartitioner.class);
            job.setReducerClass(C_W_Reducer.class);
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
        argsList.add("-minpmi");

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


