package emrSimulation;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;

import java.io.*;
import java.util.*;

public class EMRSimulator {

    public static void main(String[] args) {
        System.out.println("EMR Simulator");
        String inputPath = args[0];
        try{
            String corpus = readInputFile(inputPath);

            // Step 1
            Configuration conf = new Configuration();
            conf.set("stopWordsFile", "heb_stop_words.txt");
            conf.set("corpusPercentage", "1.0");
            conf.set("language", "hebrew");
            Map<Long,String> input = makeMapperInput(corpus);
            Step1Mapper step1Mapper = new Step1Mapper(input,conf ,new Step1Comparator());
            step1Mapper.run();
            Step1Reducer step1Reducer = new Step1Reducer(step1Mapper.getOutput(),conf);
            step1Reducer.run();
            String step1Output = step1Reducer.getOutput();

            // Step 2
            input = makeMapperInput(step1Output);
            conf = new Configuration();
            Step2Mapper step2Mapper = new Step2Mapper(input,conf, new Step1Comparator());
            step2Mapper.run();
            Step2Reducer step2Reducer = new Step2Reducer(step2Mapper.getOutput(),conf);
            step2Reducer.run();
            String step2Output = step2Reducer.getOutput();

            // Step 3
            input = makeMapperInput(step2Output);
            conf = new Configuration();
            conf.set("minPmi", "0.5");
            conf.set("relMinPmi", "0.2");
            Step3Mapper step3Mapper = new Step3Mapper(input,conf, new Step3Comparator());
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

        public Step1Mapper(Map<Long, String> _input, Configuration _conf, Comparator<String> comparator) {
            super(_input, _conf, comparator);
        }

        enum Language {
            english,
            hebrew

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
        private double corpusPercentage;
        private Language language;

        @Override
        protected void map(Long key, String value, Context context) throws IOException, InterruptedException {
            if(Math.random() > corpusPercentage){
                return;
            }
            String[] tokens = value.toString().split("\\s+");

            if(language == Language.english){
                tokens[W1_INDEX] = tokens[W1_INDEX].toLowerCase();
                tokens[W2_INDEX] = tokens[W2_INDEX].toLowerCase();

                if(!tokens[W1_INDEX].matches("[a-z]*") || !tokens[W2_INDEX].matches("[a-z]*")){
                    return;
                }
            } else {
                if(!tokens[W1_INDEX].matches("[א-ת]*") || !tokens[W2_INDEX].matches("[א-ת]*")){
                    return;
                }
            }

            // skip stop words
            if (stopWords.contains(tokens[W1_INDEX]) || stopWords.contains(tokens[W2_INDEX])) {return;}

            String decade = tokens[DECADE_INDEX];
            decade = decade.substring(0, decade.length() - 1) + "0";
            String w1 = tokens[W1_INDEX];
            String w2 = tokens[W2_INDEX];

            Long outValue = Long.parseLong(tokens[COUNT_OVERALL_INDEX]);
            String outKey = "%s,_,_".formatted(decade);
            context.write(outKey, outValue);
            outKey = "%s,%s,_".formatted(decade,w1);
            context.write(outKey, outValue);
            outKey = "%s,%s,%s".formatted(decade,w1,w2);
            context.write(outKey, outValue);
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outKey = new Text();
            outValue = new LongWritable();

            // get configuration
            Configuration conf = context.getConfiguration();
            corpusPercentage = conf.getDouble("corpusPercentage", 1.0);
            language = Language.valueOf(conf.get("language"));
            String stopWordsFile = conf.get("stopWordsFile");

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

    public static class Step1Comparator implements Comparator<String> {

        private static final int DECADE_INDEX = 0;
        private static final int W1_INDEX = 1;
        private static final int W2_INDEX = 2;

        @Override
        public int compare(String a, String b) {
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

    public static class Step1Reducer extends SimulatedReducer<String,Long,String,String>{

        public Step1Reducer(Map<String, Iterable<Long>> _input, Configuration conf) {
            super(_input,conf);
        }

        private static final int KEY_DECADE_INDEX = 0;
        private static final int KEY_W1_INDEX = 1;
        private static final int KEY_W2_INDEX = 2;
        Text outkey;
        Text outValue;
        String currentW1;
        long c_w1;
        String currentDecade;
        long N;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outkey = new Text();
            outValue = new Text();
        }

        @Override
        protected void reduce(String key, Iterable<Long> values, Context context) throws IOException, InterruptedException {
            String[] keyTokens = key.toString().split(",");
            String decade = keyTokens[KEY_DECADE_INDEX];
            String w1 = keyTokens[KEY_W1_INDEX];
            String w2 = keyTokens[KEY_W2_INDEX];

            if(currentDecade == null || !currentDecade.equals(decade)){
                currentDecade = decade;
                currentW1 = null;
                N = 0;
            }
            if(currentW1 == null || !currentW1.equals(w1)){
                currentW1 = w1;
                c_w1 = 0;
            }

            long counter = 0;

            // count N in decade
            if(w1.equals("_") && w2.equals("_")) {
                for (Long value : values) {
                    counter += value;
                }
                N = counter;
            }
            // count c_w1 in decade
            else if(w2.equals("_")) {
                for (Long value : values) {
                    counter += value;
                }
                c_w1 = counter;
            }
            // count c_w1_w2 in decade
            else {
                for (Long value : values) {
                    counter += value;
                }
                String outkey = "%s,%s,%s".formatted(decade,w1,w2);
                String outValue = "%s,%s,%s".formatted(counter,N,c_w1);
                context.write(outkey, outValue);
            }
        }
    }

    public static class Step2Mapper extends SimulatedMapper<Long,String,String,String>{
        public Step2Mapper(Map<Long, String> _input, Configuration _conf) {
            super(_input, _conf);
        }

        public Step2Mapper(Map<Long, String> _input, Configuration _conf, Comparator<String> comparator) {
            super(_input, _conf, comparator);
        }
        // <KEY INDEXES>
        private static final int DECADE_KEY_INDEX = 0;
        private static final int W1_KEY_INDEX = 1;
        private static final int W2_KEY_INDEX = 2;

        // </KEY INDEXES>

        private Text outKey;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outKey = new Text();
        }

        @Override
        protected void map(Long key, String value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\\s+");
            String[] keyTokens = values[0].split(",");

            String decade = keyTokens[DECADE_KEY_INDEX];
            String w1 = keyTokens[W1_KEY_INDEX];
            String w2 = keyTokens[W2_KEY_INDEX];

            value = values[1];
            String outKey = ("%s,_,_".formatted(decade));
            context.write(outKey, value);
            outKey = ("%s,%s,_".formatted(decade,w2));
            context.write(outKey, value);
            outKey = ("%s,%s,%s".formatted(decade,w2,w1));
            context.write(outKey, value);
        }

    }

    public static class Step2Reducer extends SimulatedReducer<String,String,String,Double>{
        public Step2Reducer(Map<String, Iterable<String>> _input, Configuration _conf) {
            super(_input, _conf);
        }

        private static final int KEY_DECADE_INDEX = 0;
        private static final int KEY_W1_INDEX = 1;
        private static final int KEY_W2_INDEX = 2;
        private static final int VALUE_C_W1_W2_INDEX = 0;
        private static final int VALUE_N_INDEX = 1;
        private static final int VALUE_C_W1_INDEX = 2;
        private static final int VALUE_C_W2_INDEX = 3;

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
        protected void reduce(String key, Iterable<String> values, Context context) throws IOException, InterruptedException {

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

            // Don't count N in decade
            if(w1.equals("_") && w2.equals("_")) {
                return;
            }
            // count c_w2 in decade
            else if(w2.equals("_")) {
                for (String value : values) {
                    String[] valueTokens = value.toString().split(",");
                    counter += Long.parseLong(valueTokens[VALUE_C_W1_W2_INDEX]);
                }
                c_w2 = counter;
            }
            // emit c_w1_w2 in decade
            else {
                Iterator<String> valuesIterator = values.iterator();
                String value = valuesIterator.next();
                String outKey = "%s,%s,%s".formatted(decade,w2,w1);

                String[] valueTokens = value.toString().split(",");
                double c_w1_w2 = Double.parseDouble(valueTokens[VALUE_C_W1_W2_INDEX]);
                double N = Double.parseDouble(valueTokens[VALUE_N_INDEX]);
                double c_w1 = Double.parseDouble(valueTokens[VALUE_C_W1_INDEX]);

                if(Math.log(c_w1_w2/N) == 0) return; // division by zero

                double npmi = calculateNPMI(c_w1_w2, N, c_w1, c_w2);

//                outValue.set(npmi);
                context.write(outKey, npmi);

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

    public static class Step3Mapper extends SimulatedMapper<Long,String,String,String> {
        public Step3Mapper(Map<Long, String> _input, Configuration _conf) {
            super(_input, _conf);
        }

        public Step3Mapper(Map<Long, String> _input, Configuration _conf, Comparator<String> comparator) {
            super(_input, _conf, comparator);
        }


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
        protected void map(Long key, String value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\\s+");

            String[] keyTokens = values[0].split(",");

            String decade = keyTokens[KEY_DECADE_INDEX];
            String w1 = keyTokens[KEY_W1_INDEX];
            String w2 = keyTokens[KEY_W2_INDEX];
            double npmi = Double.parseDouble(values[1]);

            String outKey = "%s,_,_,_".formatted(decade);
            String outValue = String.valueOf(npmi);
            context.write(outKey, outValue);
            outKey = "%s,%s,%s,%s".formatted(decade,w1,w2,npmi);
            outValue = "";
            context.write(outKey, outValue);
        }
    }

    public static class Step3Comparator implements Comparator<String> {

        private static final int DECADE_INDEX = 0;
        private static final int W1_INDEX = 1;
        private static final int W2_INDEX = 2;
        private static final int NPMI_INDEX = 3;


        @Override
        public int compare(String a, String b) {
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

    public static class Step3Reducer extends SimulatedReducer<String,String,String,String> {
        public Step3Reducer(Map<String, Iterable<String>> _input, Configuration _conf) {
            super(_input, _conf);
        }

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
        protected void reduce(String key, Iterable<String> values, Context context) throws IOException, InterruptedException {
            String[] keyTokens = key.toString().split(",");
            String decade = keyTokens[KEY_DECADE_INDEX];

            if(currentDecade == null || !currentDecade.equals(decade)){
                currentDecade = decade;
            }

            if(keyTokens[KEY_NPMI_INDEX].equals("_")){
                for (String value : values){
                    totalNpmiInDecade += Double.parseDouble(value.toString());
                }
            } else {
                double npmi = Double.parseDouble(keyTokens[KEY_NPMI_INDEX]);
                double relNpmi = npmi / totalNpmiInDecade;

                if(npmi < minPmi && relNpmi < relMinPmi){
                    return;
                }
                String outKey = "%s".formatted(key.toString().replaceAll(","," "));
                context.write(outKey, "");
            }
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
