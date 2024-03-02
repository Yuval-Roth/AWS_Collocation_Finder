import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * emits the following format:
 *    decade,w1,_ -> w1,w2,count_overall,bigram_count_in_decade
 *    decade,_,w2 -> w1,w2,count_overall,bigram_count_in_decade
 */
public class Job2 extends Reducer<IntWritable, Text, Text, Text> {

    private static final int W1_VALUE_INDEX = 0;
    private static final int W2_VALUE_INDEX = 1;
    private static final int COUNT_OVERALL_VALUE_INDEX = 2;

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        int bigramCount = 0;
        for (Text value : values) {
            String[] valueTokens = value.toString().split(",");
            bigramCount += Integer.parseInt(valueTokens[COUNT_OVERALL_VALUE_INDEX]);
        }
        for(Text value : values){
            String[] valueTokens = value.toString().split(",");
            String w1 = valueTokens[W1_VALUE_INDEX];
            String w2 = valueTokens[W2_VALUE_INDEX];
            String countOverall = valueTokens[COUNT_OVERALL_VALUE_INDEX];
            String valueOut = String.format("%s,%s,%s,%d", w1, w2, countOverall, bigramCount);
            context.write(new Text(String.format("%d,%s,_", key.get(), w1)), new Text(valueOut));
            context.write(new Text(String.format("%d,_,%s", key.get(), w2)), new Text(valueOut));
        }
    }
}

