package emrSimulation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

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
            Job job = Job.getInstance(conf, "Step1");
            conf.set("stopWordsFile", "heb_stop_words.txt");
            conf.set("corpusPercentage", "1.0");
            conf.set("language", "hebrew");
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
            Map<String, Iterable<String>> Step2Output = step3Mapper.getOutput();
            Step3Reducer step3Reducer = new Step3Reducer(Step2Output,conf);
            step3Reducer.run();
            String step3Output = step3Reducer.getOutput(new Step3Comparator());
            System.out.println(step3Output);

        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public static class Step1Mapper extends SimulatedMapper<Long,String,String,Long> {
        public Step1Mapper(Map<Long, String> _input, Configuration conf) {
            super(_input,conf);
        }

        enum Language {
            english,
            hebrew

        }

        @Override
        protected void setup(Mapper<Long, String, String, Long>.Context context) throws IOException, InterruptedException {

        }

        @Override
        protected void map(Long key, String value, Mapper<Long, String, String, Long>.Context context) throws IOException, InterruptedException {

        }


    }

    public static class Step1Reducer extends SimulatedReducer<String,Long,String,Long>{

        public Step1Reducer(Map<String, Iterable<Long>> _input, Configuration conf) {
            super(_input,conf);
        }



        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

        }

        @Override
        protected void reduce(String key, Iterable<Long> values, Context context) throws IOException, InterruptedException {


        }
    }

    public static class Step2Mapper extends SimulatedMapper<Long,String,String,String>{
        public Step2Mapper(Map<Long, String> _input, Configuration _conf) {
            super(_input, _conf);
        }


        @Override
        protected void setup(Mapper<Long, String, String, String>.Context context) throws IOException, InterruptedException {

        }

        @Override
        protected void map(Long key, String value, Mapper<Long, String, String, String>.Context context) throws IOException, InterruptedException {

        }

    }

    public static class Step2Reducer extends SimulatedReducer<String,String,String,String>{
        public Step2Reducer(Map<String, Iterable<String>> _input, Configuration _conf) {
            super(_input, _conf);
        }


        @Override
        protected void setup(Reducer<String, String, String, String>.Context context) throws IOException, InterruptedException {

        }

        @Override
        protected void reduce(String key, Iterable<String> values, Reducer<String, String, String, String>.Context context) throws IOException, InterruptedException {

        }

    }

    public static class Step3Mapper extends SimulatedMapper<Long,String,String,String> {
        public Step3Mapper(Map<Long, String> _input, Configuration _conf) {
            super(_input, _conf);
        }


        @Override
        protected void setup(Mapper<Long, String, String, String>.Context context) throws IOException, InterruptedException {

        }

        @Override
        protected void map(Long key, String value, Mapper<Long, String, String, String>.Context context) throws IOException, InterruptedException {

        }


    }

    public static class Step3Comparator implements Comparator<String> {

        @Override
        public int compare(String o1, String o2) {
            return 0;
        }
    }

    public static class Step3Reducer extends SimulatedReducer<String,String,String,String> {
        public Step3Reducer(Map<String, Iterable<String>> _input, Configuration _conf) {
            super(_input, _conf);
        }


        @Override
        protected void setup(Reducer<String, String, String, String>.Context context) throws IOException, InterruptedException {

        }

        @Override
        protected void reduce(String key, Iterable<String> values, Reducer<String, String, String, String>.Context context) throws IOException, InterruptedException {

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
