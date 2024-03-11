package emrSimulation;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class EMRSimulator {

    public static Map<String,String> sfs = new HashMap<>();

    public static void main(String[] args) {
        System.out.println("EMR Simulator");
        String inputPath = args[0];
        try{
            String corpus = readInputFile(inputPath);

            // Step 1
            Configuration conf = new Configuration();
            conf.set("stopWordsFile", "stop_words.txt");
            Map<Long,String> input = makeMapperInput(corpus);
            Step1Mapper step1Mapper = new Step1Mapper(input,conf);
            step1Mapper.run();
            Step1Reducer step1Reducer = new Step1Reducer(step1Mapper.getOutput(),conf);
            step1Reducer.run();
            String step1Output = step1Reducer.getOutput();

            // Step 2
            input = makeMapperInput(step1Output);
            conf = new Configuration();
            conf.set("minPmi", "0");
            Step2Mapper step2Mapper = new Step2Mapper(input,conf);
            step2Mapper.run();
            Step2Reducer step2Reducer = new Step2Reducer(step2Mapper.getOutput(),conf);
            step2Reducer.run();
            String step2Output = step2Reducer.getOutput();
            System.out.println(step2Output);

        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public static class Step1Mapper extends SimulatedMapper<Long,String,String,Long> {

        public Step1Mapper(Map<Long, String> _input, Configuration conf) {
            super(_input,conf);
        }

        private static final String BUCKET_NAME = "distributed-systems-2024-bucket-yuval-adi";
        private static final int W1_INDEX = 0;
        private static final int W2_INDEX = 1;
        private static final int DECADE_INDEX = 2;
        private static final int COUNT_OVERALL_INDEX = 3;
        private Set<String> stopWords;
        private AmazonS3 s3;
        private Text outKey;
        private LongWritable outValue;

        @Override
        protected void map(Long key, String value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\s+");

            // remove tags from words if they exist
            int index;
            if ((index = tokens[W1_INDEX].indexOf("_")) != -1){
                tokens[W1_INDEX] = tokens[W1_INDEX].substring(0,index);
            }
            if ((index = tokens[W2_INDEX].indexOf("_")) != -1){
                tokens[W2_INDEX] = tokens[W2_INDEX].substring(0,index);
            }
            if(tokens[W1_INDEX].isEmpty() || tokens[W2_INDEX].isEmpty()){
                return;
            }

            // skip stop words
            if (stopWords.contains(tokens[W1_INDEX]) || stopWords.contains(tokens[W2_INDEX])) {
                return;
            }

            String decade = tokens[DECADE_INDEX];
            decade = decade.substring(0, decade.length() - 1) + "0";
            String w1 = tokens[W1_INDEX];
            String w2 = tokens[W2_INDEX];

            long outValue = Long.parseLong(tokens[COUNT_OVERALL_INDEX]);
            String outKey = "%s,%s,%s".formatted(decade,w1,w2);
            context.write(outKey,outValue);
            outKey = "%s,%s,_".formatted(decade,w1);
            context.write(outKey,outValue);
            outKey = "%s,_,%s".formatted(decade,w2);
            context.write(outKey,outValue);
            outKey = "%s,_,_".formatted(decade);
            context.write(outKey,outValue);
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outKey = new Text();
            outValue = new LongWritable();
            String stopWordsFile = context.getConfiguration().get("stopWordsFile");
            s3 = AmazonS3Client.builder().withRegion(Regions.US_WEST_2).build();
            String stopWordsStr = downloadSmallFileFromS3(stopWordsFile);
            stopWords = new HashSet<>();
            stopWordsStr.lines().forEach(stopWords::add);
        }

        private String downloadSmallFileFromS3(String key) {

            S3Object r = s3.getObject(new GetObjectRequest(BUCKET_NAME, "hadoop/" + key));

            // get file from response
            byte[] file = {};
            try {
                file = r.getObjectContent().readAllBytes();
            } catch (IOException e) {
                throw new RuntimeException("Failed to read file from S3", e);
            }

            return new String(file);
        }
    }

    public static class Step1Reducer extends SimulatedReducer<String,Long,String,Long>{

        public Step1Reducer(Map<String, Iterable<Long>> _input, Configuration conf) {
            super(_input,conf);
        }

        private static final int KEY_DECADE_INDEX = 0;
        private static final int KEY_W1_INDEX = 1;
        private static final int KEY_W2_INDEX = 2;
        FileSystem fs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            fs = FileSystem.get(context.getConfiguration());
        }

        @Override
        protected void reduce(String key, Iterable<Long> values, Context context) throws IOException, InterruptedException {

            Path folderPath = new Path("hdfs:///step1/");
            fs.mkdirs(folderPath);
            String[] keyTokens = key.toString().split(",");
            String decade = keyTokens[KEY_DECADE_INDEX];
            String w1 = keyTokens[KEY_W1_INDEX];
            String w2 = keyTokens[KEY_W2_INDEX];

            long counter = 0;
            for (Long value : values) {
                counter += value;
            }

            // <decade,w1,w2> -- count C(w1,w2) in decade
            if(! (w1.equals("_") || w2.equals("_"))){
                context.write(key, counter);
            } else{
                Path filePath;

                // <decade,_,_> -- count bigrams in decade (N)
                if(keyTokens[KEY_W1_INDEX].equals("_") && keyTokens[KEY_W2_INDEX].equals("_")){
                    //"hdfs:///jobs1/1990-_-_"
                    filePath = new Path(folderPath, "%s-_-_".formatted(decade));
                }
                // <decade,w1,_> -- count c(w1) in decade
                else if(keyTokens[KEY_W2_INDEX].equals("_")) {
                    //"hdfs:///jobs1/1990-w1-_"
                    filePath = new Path(folderPath, "%s-%s-_".formatted(decade,w1));
                }
                // <decade,_,w2> -- count c(w2) in decade
                else {
                    //"hdfs:///jobs1/1990-_-w2"
                    filePath = new Path(folderPath, "%s-_-%s".formatted(decade,w2));
                }

//                fs.create(filePath).writeUTF(String.valueOf(counter));
                sfs.put(filePath.toString(), String.valueOf(counter));
            }
        }
    }

    public static class Step2Mapper extends SimulatedMapper<Long,String,String,String>{

        public Step2Mapper(Map<Long, String> _input, Configuration _conf) {
            super(_input, _conf);
        }

        // <KEY INDEXES>
        private static final int KEY_DECADE_INDEX = 0;
        private static final int VALUE_W1_INDEX = 0;
        private static final int VALUE_W2_INDEX = 1;
        private static final int VALUE_C_W1_W2_INDEX = 2;
        private static final int VALUE_BIGRAM_COUNT_IN_DECADE_INDEX = 3;
        private static final int VALUE_C_W1_INDEX = 4;
        private static final int VALUE_C_W2_INDEX = 5;
        // </KEY INDEXES>

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

        }

        @Override
        protected void map(Long key, String value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\\s+");

            String[] keyTokens = values[0].split(",");
            String[] valueTokens = values[1].split(",");

            String outKey = "%s,%s,%s".formatted(
                    keyTokens[KEY_DECADE_INDEX],
                    valueTokens[VALUE_W1_INDEX],
                    valueTokens[VALUE_W2_INDEX]);
            String outValue = "%s,%s,%s,%s".formatted(
                    valueTokens[VALUE_C_W1_W2_INDEX],
                    valueTokens[VALUE_BIGRAM_COUNT_IN_DECADE_INDEX],
                    valueTokens[VALUE_C_W1_INDEX],
                    valueTokens[VALUE_C_W2_INDEX]);
            context.write(outKey, outValue);
        }
    }

    public static class Step2Reducer extends SimulatedReducer<String,String,String,String>{

        // <VALUE INDEXES>
        private static final int VALUE_C_W1_W2_INDEX = 0;
        private static final int VALUE_BIGRAM_COUNT_IN_DECADE_INDEX = 1;
        private static final int VALUE_C_W1_INDEX = 2;
        private static final int VALUE_C_W2_INDEX = 3;
        // </VALUE INDEXES>

        Text outValue;
        private double minPmi;

        public Step2Reducer(Map<String, Iterable<String>> _input, Configuration _conf) {
            super(_input, _conf);
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outValue = new Text();
            minPmi = Double.parseDouble(context.getConfiguration().get("minPmi"));
        }

        @Override
        protected void reduce(String key, Iterable<String> values, Context context) throws IOException, InterruptedException {
            double c_w1_w2 = 0;
            double N = 0;
            double c_w1 = 0;
            double c_w2 = 0;

            for(String value : values){
                String[] valueTokens = value.toString().split(",");
                if(!valueTokens[VALUE_C_W1_W2_INDEX].equals("_")){
                    c_w1_w2 = Long.parseLong(valueTokens[VALUE_C_W1_W2_INDEX]);
                }
                else if(!valueTokens[VALUE_BIGRAM_COUNT_IN_DECADE_INDEX].equals("_")){
                    N = Long.parseLong(valueTokens[VALUE_BIGRAM_COUNT_IN_DECADE_INDEX]);
                }
                else if(!valueTokens[VALUE_C_W1_INDEX].equals("_")){
                    c_w1 = Long.parseLong(valueTokens[VALUE_C_W1_INDEX]);
                }
                else if(!valueTokens[VALUE_C_W2_INDEX].equals("_")){
                    c_w2 = Long.parseLong(valueTokens[VALUE_C_W2_INDEX]);
                }
            }
            double npmi = calculateNPMI(c_w1_w2, N, c_w1, c_w2);
            if(npmi < minPmi){
                return;
            }
            String outValue = String.valueOf(npmi);
            context.write(key, outValue);
        }

        private double calculateNPMI(double c_w1_w2, double N, double c_w1, double c_w2) {
            double pmi = Math.log(c_w1_w2) + Math.log(N) - Math.log(c_w1) - Math.log(c_w2);
            return -1 * pmi / Math.log(c_w1_w2 / N);
        }
    }



    private static Map<Long,String> makeMapperInput(String input) {
        Map<Long,String> mapperInput = new TreeMap<>();
        String[] lines = input.split("\n");

        long index = 0;
        for(String line : lines){
            mapperInput.put(index++,line);
        }
        return mapperInput;
    }

    private static String readInputFile(String inputPath) throws IOException {
        StringBuilder output = new StringBuilder();
        try(BufferedReader reader = new BufferedReader(new FileReader(inputPath))){
            String line;
            while((line = reader.readLine()) != null){
                output.append(line).append("\n");
            }
        }
        return output.toString();
    }

}
