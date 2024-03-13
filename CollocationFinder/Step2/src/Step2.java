import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;

public class Step2 {

    private static Path _inputPath;
    private static Path _outputPath;

    public static class Step2Mapper extends Mapper<LongWritable, Text, Text, Text> {

        private static final int CACHE_SIZE = 10000;
        // <KEY INDEXES>
        private static final int DECADE_KEY_INDEX = 0;
        private static final int W1_KEY_INDEX = 1;
        private static final int W2_KEY_INDEX = 2;
        // </KEY INDEXES>

        FileContext fs;
        private Text outKey;
        private Text outValue;

        private LRUCache<String, Integer> cache;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            cache = new LRUCache<>(CACHE_SIZE);
            outKey = new Text();
            outValue = new Text();
            fs = FileContext.getFileContext(context.getConfiguration());
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            Path folderPath = new Path("/step1/");
            String[] values = value.toString().split("\\s+");

            String[] keyTokens = values[0].split(",");
            String decade = keyTokens[DECADE_KEY_INDEX];
            String w1 = keyTokens[W1_KEY_INDEX];
            String w2 = keyTokens[W2_KEY_INDEX];
            String countOverall = values[1];
            Path bigramCountPath = new Path(folderPath, "%s-_-_".formatted(decade));
            Path w1CountPath = new Path(folderPath, "%s-%s-_".formatted(decade, w1));
            Path w2CountPath = new Path(folderPath, "%s-_-%s".formatted(decade, w2));

            try{
                double c_w1_w2 = Double.parseDouble(countOverall);
                double N = getValue(bigramCountPath);
                double c_w1 = getValue(w1CountPath);
                double c_w2 = getValue(w2CountPath);


                if(c_w1_w2 == N || Math.log(c_w1_w2/N) == 0.0) {return; /*0 in the denominator*/}
                if(c_w1 == 1 && c_w2 == 1 && c_w1_w2 == 1) {return;}

                double npmi = calculateNPMI(c_w1_w2, N, c_w1, c_w2);

                outKey.set(decade);
                outValue.set("%s,%s,%s".formatted(w1,w2,npmi));
                context.write(outKey, outValue);
            } catch(IllegalStateException ignored){}
        }

        private double calculateNPMI(double c_w1_w2, double N, double c_w1, double c_w2) {
            double pmi = Math.log(c_w1_w2) + Math.log(N) - Math.log(c_w1) - Math.log(c_w2);
            return -1 * pmi / Math.log(c_w1_w2 / N);
        }

        private int getValue(Path path) {
            int value;
            int tries = 0;
            if(cache.contains(path.toString())){
                value = cache.get(path.toString());
            } else {
                String str = "";
                boolean success = false;
                do{
                    try{
                        BufferedInputStream reader = new BufferedInputStream(fs.open(path));
                        str = new String(reader.readAllBytes());
                        reader.close();
                        success = ! str.isBlank();
                    } catch (IOException ignored){
                        tries++;
                    }
                } while (! success && tries < 10);
                if(tries == 10){
                    throw new IllegalStateException("Too many tries to read from file");
                }
                value = Integer.parseInt(str);
                cache.put(path.toString(), value);
            }
            return value;
        }
    }

    public static class Step2Reducer extends Reducer<Text, Text, Text, Text> {

        private static int NPMI_VALUE_INDEX = 2;
        private FileSystem fs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            fs = FileSystem.get(context.getConfiguration());
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Path folderPath = new Path("/step3/");
            fs.mkdirs(folderPath);

            double npmiTotalInDecade = 0;
            for(Text value : values){
                String[] valueTokens = value.toString().split(",");
                npmiTotalInDecade += Double.parseDouble(valueTokens[NPMI_VALUE_INDEX]);
                context.write(key, value);
            }

            Path filePath = new Path(folderPath, key.toString());
            boolean success;
            do{
                try{
                    OutputStream s = fs.create(filePath);
                    s.write(String.valueOf(npmiTotalInDecade).getBytes());
                    s.close();
                    success = true;
                } catch (IOException e){
                    success = false;
                }
            } while(!success);
        }
    }

    public static class Step2Partitioner extends Partitioner<Text, Text> {
        public int getPartition(Text key, Text value, int numPartitions) {
            String[] keyTokens = key.toString().split(",");
            return Integer.parseInt(String.valueOf(keyTokens[0].charAt(2))) % numPartitions;
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
            job.setPartitionerClass(Step2Partitioner.class);
            job.setReducerClass(Step2Reducer.class);
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


