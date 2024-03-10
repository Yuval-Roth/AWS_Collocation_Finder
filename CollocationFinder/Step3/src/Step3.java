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

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class Step3 {

    private static Path _inputPath;
    private static Path _outputPath;

    public static class NPMIMapper extends Mapper<LongWritable, Text, Text, Text> {

        private static final int DECADE_KEY_INDEX = 0;
        private static final int W1_KEY_INDEX = 1;
        private static final int W2_KEY_INDEX = 2;

        private Text outKey;
        private Text outValue;

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            outKey = new Text();
            outValue = new Text();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\\s+");

            String[] keyTokens = values[0].split(",");
            double pmi = Double.parseDouble(values[1]);
            String decade = keyTokens[DECADE_KEY_INDEX];
            String w1 = keyTokens[W1_KEY_INDEX];
            String w2 = keyTokens[W2_KEY_INDEX];
            outKey.set(decade);
            outValue.set("%s,%s,%s".formatted(w1, w2, pmi));
            context.write(outKey, outValue);
        }
    }

    public static class NPMIReducer extends Reducer<Text, Text, Text, Text> {

        private static int NPMI_VALUE_INDEX = 2;
        private FileSystem fs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            fs = FileSystem.get(context.getConfiguration());
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Path folderPath = new Path("hdfs:///step3/");
            fs.mkdirs(folderPath);

            double npmiTotal = 0;
            for(Text value : values){
                String[] valueTokens = value.toString().split(",");
                double npmi = Double.parseDouble(valueTokens[NPMI_VALUE_INDEX]);
                npmiTotal += npmi;
                context.write(key, value);
            }

            Path filePath = new Path(folderPath, key.toString());
            fs.create(filePath).writeUTF(String.valueOf(npmiTotal));
        }
    }

    public static void main(String[] args){
        System.out.println("[DEBUG] STEP 3 started!");
        readArgs(args);
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf, "Step3");
            job.setJarByClass(Step3.class);
            job.setMapperClass(NPMIMapper.class);
            job.setPartitionerClass(DecadesPartitioner.class);
            job.setReducerClass(NPMIReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
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


