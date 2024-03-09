import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCount {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        private static final int TOKENS_PER_LINE = 5;
        private static final int W1_INDEX = 0;
        private static final int W2_INDEX = 1;
        private static final int DECADE_INDEX = 2;
        private static final int COUNT_OVERALL_INDEX = 3;
        private static final int DISTINCT_BOOKS_COUNT_INDEX = 4;
        private static final String BUCKET_NAME = "distributed-systems-2024-bucket-yuval-adi";

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(" ");
            String decade = tokens[DECADE_INDEX];
            Text outKey = new Text(decade.substring(0,decade.length()-1)+"0");
            Text outValue = new Text (tokens[W1_INDEX] +","+ tokens[W2_INDEX]+","+ tokens[COUNT_OVERALL_INDEX]);
            context.write(outKey, outValue);
        }
    }

    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {

        private static final int W1_VALUE_INDEX = 0;
        private static final int W2_VALUE_INDEX = 1;
        private static final int COUNT_OVERALL_VALUE_INDEX = 2;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            long bigramCountInDecade = 0;
            for (Text value : values) {
                String[] valueTokens = value.toString().split(",");
                bigramCountInDecade += Long.parseLong(valueTokens[COUNT_OVERALL_VALUE_INDEX]);
            }
            for(Text value : values){
                String[] valueTokens = value.toString().split(",");
                String w1 = valueTokens[W1_VALUE_INDEX];
                String w2 = valueTokens[W2_VALUE_INDEX];
                String countOverall = valueTokens[COUNT_OVERALL_VALUE_INDEX];
                String valueOut = String.format("%s,%s,%s,%d", w1, w2, countOverall, bigramCountInDecade);
                context.write(key, new Text(valueOut));
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

//        For n_grams S3 files.
//        Note: This is English version and you should change the path to the relevant one
//        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/3gram/data"));

        FileInputFormat.addInputPath(job, new Path("s3://distributed-systems-2024-bucket-yuval-adi/hadoop/inputs/small"));
        FileOutputFormat.setOutputPath(job, new Path("s3://distributed-systems-2024-bucket-yuval-adi/hadoop/output_word_count"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
