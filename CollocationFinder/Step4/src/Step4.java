import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import utils.DecadesPartitioner;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class Step4 {

    private static Path _inputPath;
    private static Path _outputPath;
    private static Double _relMinPmi;

    public static class RelNPMIMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final int VALUE_NPMI_INDEX = 2;
        private Text outKey;
        private Text outValue;
        private FileSystem fs;
        private double relMinPmi;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            relMinPmi = Double.parseDouble(context.getConfiguration().get("relMinPmi"));
            outKey = new Text();
            outValue = new Text("");
            fs = FileSystem.get(context.getConfiguration());
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\\s+");

            Path folderPath = new Path("hdfs:///step3/");
            Path filePath = new Path(folderPath, key.toString());
            double npmiTotalInDecade = 0;
            try (BufferedInputStream reader = new BufferedInputStream(fs.open(filePath))) {
                npmiTotalInDecade = Double.parseDouble(new String(reader.readAllBytes()));
            }
            String[] valueTokens = value.toString().split(",");
            double npmi = Double.parseDouble(valueTokens[VALUE_NPMI_INDEX]);
            double relNpmi = npmi / npmiTotalInDecade;

            if (relNpmi < relMinPmi) {
                return;
            }

            outKey.set("%s %s".formatted(values[0], values[1].replace(",", " ")));
            context.write(outKey, outValue);
        }
    }
    public static class DescendingComparator extends WritableComparator {

        private static final int DECADE_INDEX = 0;
        private static final int W1_INDEX = 1;
        private static final int W2_INDEX = 2;
        private static final int NPMI_INDEX = 3;

        DescendingComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            String[] aTokens = a.toString().split("\\s+");
            String[] bTokens = b.toString().split("\\s+");
            int num;
            if((num = aTokens[DECADE_INDEX].compareTo(bTokens[DECADE_INDEX])) != 0){
                return num;
            }
            else if ((num = aTokens[NPMI_INDEX].compareTo(bTokens[NPMI_INDEX])) != 0){
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
        System.out.println("[DEBUG] STEP 4 started!");
        readArgs(args);
        Configuration conf = new Configuration();
        conf.set("relMinPmi", String.valueOf(_relMinPmi));
        try {
            Job job = Job.getInstance(conf, "Step4");
            job.setJarByClass(Step4.class);
            job.setMapperClass(RelNPMIMapper.class);
            job.setPartitionerClass(DecadesPartitioner.class);
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


