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
import utils.DescendingComparator;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;

public class Step4 {

    private static Path _inputPath;
    private static Path _outputPath;
    private static Double _relMinPmi;

    public static class RelNPMIMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text outKey;
        private Text outValue;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outKey = new Text();
            outValue = new Text("");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\\s+");

            outKey.set(values[0]);
            outValue.set(values[1]);
            context.write(outKey, outValue);
        }
    }

    public static class RelNPMIReducer extends Reducer<Text, Text, Text, Text> {

        private static final int W1_VALUE_INDEX = 0;
        private static final int W2_VALUE_INDEX = 1;
        private static final int NPMI_VALUE_INDEX = 2;

        private double relMinPmi;
        private Text outKey;
        private Text outValue;
        private FileSystem fs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            relMinPmi = Double.parseDouble(context.getConfiguration().get("relMinPmi"));
            outKey = new Text();
            outValue = new Text("");
            fs = FileSystem.get(context.getConfiguration());
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Path folderPath = new Path("hdfs:///step3/");
            Path filePath = new Path(folderPath, key.toString());
            double npmiTotal = 0;
            try(BufferedInputStream reader = new BufferedInputStream(fs.open(filePath))){
                npmiTotal = Double.parseDouble(new String(reader.readAllBytes()));
            }

            for(Text value: values){
                String[] valueTokens = value.toString().split(",");
                double npmi = Double.parseDouble(valueTokens[NPMI_VALUE_INDEX]);
                double relNpmi = npmi / npmiTotal;

                if(relNpmi < relMinPmi){
                    continue;
                }

                outKey.set("%s %s %s %s".formatted(key.toString(),
                        valueTokens[W1_VALUE_INDEX],
                        valueTokens[W2_VALUE_INDEX],
                        npmi));
                context.write(outKey, outValue);
            }
        }
    }

    public static void main(String[] args){
        System.out.println("[DEBUG] STEP 4 started!");
        readArgs(args);
        Configuration conf = new Configuration();
        conf.set("relMinPmi", String.valueOf(_relMinPmi));
        try {
            Job job = Job.getInstance(conf, "Step4");
            job.setJarByClass(Step4.class);
            job.setMapperClass(RelNPMIMapper.class);
            job.setPartitionerClass(DecadesPartitioner.class);
            job.setReducerClass(RelNPMIReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setSortComparatorClass(DescendingComparator.class);
            FileInputFormat.addInputPath(job, _inputPath);
            FileOutputFormat.setOutputPath(job, _outputPath);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
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


