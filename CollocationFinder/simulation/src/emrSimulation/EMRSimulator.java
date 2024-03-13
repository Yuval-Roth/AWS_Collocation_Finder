package emrSimulation;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
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
            Job job = Job.getInstance(conf, "Step1");
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
            Step2Mapper step2Mapper = new Step2Mapper(input,conf);
            step2Mapper.run();
            Step2Reducer step2Reducer = new Step2Reducer(step2Mapper.getOutput(),conf);
            step2Reducer.run();
            String step2Output = step2Reducer.getOutput();

            // Step 3
            input = makeMapperInput(step2Output);
            conf = new Configuration();
            conf.set("minPmi", "0.5");
            conf.set("relMinPmi", "0.2");
            Step3Mapper step3Mapper = new Step3Mapper(input,conf);
            step3Mapper.run();
            Step3Reducer step3Reducer = new Step3Reducer(step3Mapper.getOutput(),conf);
            step3Reducer.run();
            String step3Output = step3Reducer.getOutput();
            System.out.println(step3Output);

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
        private LongWritable outValue;
        private Text outKey;

        @Override
        protected void map(Long key, String value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\s+");

            // remove tags from words if they exist
            int index;
            if ((index = tokens[W1_INDEX].indexOf("_")) != -1){
                tokens[W1_INDEX] = tokens[W1_INDEX].substring(0,index).trim();
            }
            if ((index = tokens[W2_INDEX].indexOf("_")) != -1){
                tokens[W2_INDEX] = tokens[W2_INDEX].substring(0,index).trim();
            }
            if(tokens[W1_INDEX].isEmpty() || tokens[W2_INDEX].isEmpty()){
                return;
            }

            if (isBadInput(tokens))
                return;

            String decade = tokens[DECADE_INDEX];
            decade = decade.substring(0, decade.length() - 1) + "0";
            String w1 = tokens[W1_INDEX];
            String w2 = tokens[W2_INDEX];

//            outValue.set(Long.parseLong(tokens[COUNT_OVERALL_INDEX]));
//            outKey.set("%s,%s,%s".formatted(decade,w1,w2));
//            context.write(outKey, outValue);
//            outKey.set("%s,%s,_".formatted(decade,w1));
//            context.write(outKey, outValue);
//            outKey.set("%s,_,%s".formatted(decade,w2));
//            context.write(outKey, outValue);
//            outKey.set("%s,_,_".formatted(decade));
//            context.write(outKey, outValue);

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

        private static boolean isBadInput(String[] tokens) {
            String chars = "AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzקראטוןםפשדגכעיחלךףזסבהנמצתץ";
            String numbers = "0123456789";

            return tokens.length < 4 ||
                    isInvalid(tokens[DECADE_INDEX],numbers) ||
                    isInvalid(tokens[COUNT_OVERALL_INDEX],numbers) ||
                    isInvalid(tokens[W1_INDEX],chars) ||
                    isInvalid(tokens[W2_INDEX],chars);

        }

        private static boolean isInvalid(String w,String chars) {
            char[] _w = w.toCharArray();
            for (int i = 0; i < w.length(); i++) {
                if(chars.indexOf(_w[i]) == -1){
                    return true;
                }
            }
            return false;
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
//            fs.mkdirs(folderPath);
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

                sfs.put(filePath.toString(),String.valueOf(counter));
//                OutputStream s = fs.create(filePath);
//                s.write(String.valueOf(counter).getBytes());
//                s.close();
            }
        }
    }

    public static class Step2Mapper extends SimulatedMapper<Long,String,String,String>{

        public Step2Mapper(Map<Long, String> _input, Configuration _conf) {
            super(_input, _conf);
        }

        // <KEY INDEXES>
        private static final int CACHE_SIZE = 10000;
        // <KEY INDEXES>
        private static final int DECADE_KEY_INDEX = 0;
        private static final int W1_KEY_INDEX = 1;
        private static final int W2_KEY_INDEX = 2;
        // </KEY INDEXES>

        FileSystem fs;
        private Text outKey;
        private Text outValue;

//        private LRUCache<String, String> cache;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
//            cache = new LRUCache<>(CACHE_SIZE);
            outKey = new Text();
            outValue = new Text();
            fs = FileSystem.get(context.getConfiguration());
        }

        @Override
        protected void map(Long key, String value, Context context) throws IOException, InterruptedException {

            Path folderPath = new Path("hdfs:///step1/");
            String[] values = value.toString().split("\\s+");

            String[] keyTokens = values[0].split(",");
            String decade = keyTokens[DECADE_KEY_INDEX];
            String w1 = keyTokens[W1_KEY_INDEX];
            String w2 = keyTokens[W2_KEY_INDEX];
            String countOverall = values[1];
            Path bigramCountPath = new Path(folderPath, "%s-_-_".formatted(decade));
            Path w1CountPath = new Path(folderPath, "%s-%s-_".formatted(decade, w1));
            Path w2CountPath = new Path(folderPath, "%s-_-%s".formatted(decade, w2));

            double c_w1_w2 = Double.parseDouble(countOverall);
            double N = Double.parseDouble(getValue(bigramCountPath));
            double c_w1 = Double.parseDouble(getValue(w1CountPath));
            double c_w2 = Double.parseDouble(getValue(w2CountPath));

            if(c_w1_w2 == N || Math.log(c_w1_w2/N) == 0.0) {return; /*0 in the denominator*/}
            if(c_w1 == 1 && c_w2 == 1 && c_w1_w2 == 1) {return;}

            double npmi = calculateNPMI(c_w1_w2, N, c_w1, c_w2);


            context.write(decade, "%s,%s,%s".formatted(w1,w2,npmi));

//            outKey.set(decade);
//            outValue.set("%s,%s,%s".formatted(w1,w2,npmi));
//            context.write(outKey, outValue);
        }

        private double calculateNPMI(double c_w1_w2, double N, double c_w1, double c_w2) {
            double pmi = Math.log(c_w1_w2) + Math.log(N) - Math.log(c_w1) - Math.log(c_w2);
            return -1 * pmi / Math.log(c_w1_w2 / N);
        }

        private String getValue(Path path) throws IOException {
            String value;
            value = sfs.get(path.toString());
//            if(cache.contains(path.toString())){
//                value = cache.get(path.toString());
//            } else {
//                BufferedInputStream reader = new BufferedInputStream(fs.open(path));
//                value = new String(reader.readAllBytes());
//                reader.close();
//                cache.put(path.toString(), value);
//            }
            return value;
        }
    }

    public static class Step2Reducer extends SimulatedReducer<String,String,String,String>{

        public Step2Reducer(Map<String, Iterable<String>> _input, Configuration _conf) {
            super(_input, _conf);
        }
        // <VALUE INDEXES>
        private static int NPMI_VALUE_INDEX = 2;
        private FileSystem fs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            fs = FileSystem.get(context.getConfiguration());
        }

        @Override
        protected void reduce(String key, Iterable<String> values, Context context) throws IOException, InterruptedException {

            Path folderPath = new Path("hdfs:///step3/");
//            fs.mkdirs(folderPath);

            double npmiTotalInDecade = 0;
            for(String value : values){
                String[] valueTokens = value.toString().split(",");
                npmiTotalInDecade += Double.parseDouble(valueTokens[NPMI_VALUE_INDEX]);
                context.write(key, value);
            }

            sfs.put(key,String.valueOf(npmiTotalInDecade));
//            Path filePath = new Path(folderPath, key.toString());
//            boolean success;
//            do{
//                try{
//                    OutputStream s = fs.create(filePath);
//                    s.write(String.valueOf(npmiTotalInDecade).getBytes());
//                    s.close();
//                    success = true;
//                } catch (IOException e){
//                    success = false;
//                }
//            } while(!success);
        }
    }

    public static class Step3Mapper extends SimulatedMapper<Long,String,String,String> {

        public Step3Mapper(Map<Long, String> _input, Configuration _conf) {
            super(_input, _conf);
        }
        private static final int VALUE_NPMI_INDEX = 2;
        private static final int CACHE_SIZE = 100;
        private Text outKey;
        private Text outValue;
        private FileSystem fs;
        private double relMinPmi;
        private double minPmi;
//        private LRUCache<String, Double> cache;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
//            cache = new LRUCache<>(CACHE_SIZE);
            minPmi = Double.parseDouble(context.getConfiguration().get("minPmi"));
            relMinPmi = Double.parseDouble(context.getConfiguration().get("relMinPmi"));
            outKey = new Text();
            outValue = new Text("");
            fs = FileSystem.get(context.getConfiguration());
        }

        @Override
        protected void map(Long key, String value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\\s+");
            String decade = values[0];

            Path folderPath = new Path("hdfs:///step3/");
            Path filePath = new Path(folderPath, decade);

            double npmiTotalInDecade = 0;
            npmiTotalInDecade = Double.parseDouble(sfs.get(decade));
//            if(cache.contains(decade)){
//                npmiTotalInDecade = cache.get(decade);
//            } else {
//                BufferedInputStream reader = new BufferedInputStream(fs.open(filePath));
//                npmiTotalInDecade = Double.parseDouble(new String(reader.readAllBytes()));
//                cache.put(decade, npmiTotalInDecade);
//                reader.close();
//            }

            String[] valueTokens = value.toString().split(",");
            double npmi = Double.parseDouble(valueTokens[VALUE_NPMI_INDEX]);
            double relNpmi = npmi / npmiTotalInDecade;

            if (npmi < minPmi && relNpmi < relMinPmi) {
                return;
            }
            String outKey = "%s %s".formatted(values[0], values[1].replace(",", " "));
            context.write(outKey, "");

//            outKey.set("%s %s".formatted(values[0], values[1].replace(",", " ")));
//            context.write(outKey, outValue);
        }
    }

    public static class Step3Reducer extends SimulatedReducer<String,String,String,String> {

        @Override
        protected void setup(Reducer<String, String, String, String>.Context context) throws IOException, InterruptedException {

        }

        @Override
        protected void reduce(String key, Iterable<String> values, Reducer<String, String, String, String>.Context context) throws IOException, InterruptedException {
            for(String value : values){
                context.write(key, value);
            }
        }

        public Step3Reducer(Map<String, Iterable<String>> _input, Configuration _conf) {
            super(_input, _conf);
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
