import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * emits the following format:
 *      decade,w1,w2 -> w1,w2,count_overall,bigram_count_in_decade,w1_count_in_decade,_
 *      decade,w1,w2 -> w1,w2,count_overall,bigram_count_in_decade,_,w2_count_in_decade
 */
public class Job3 extends Reducer<Text, Text, Text, Text> {

    private static final int DECADE_KEY_INDEX = 0;
    private static final int W1_KEY_INDEX = 1;
    private static final int W2_KEY_INDEX = 2;
    private static final int W1_VALUE_INDEX = 0;
    private static final int W2_VALUE_INDEX = 1;
    private static final int COUNT_OVERALL_VALUE_INDEX = 2;
    private static final int BIGRAM_COUNT_IN_DECADE_INDEX = 3;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] keyTokens = key.toString().split(",");
        int wCount = 0;
        for (Text value : values) {
            String[] valueTokens = value.toString().split(",");
            wCount += Integer.parseInt(valueTokens[COUNT_OVERALL_VALUE_INDEX]);
        }
        for(Text value : values) {
            String[] valueTokens = value.toString().split(",");
            String w1 = valueTokens[W1_VALUE_INDEX];
            String w2 = valueTokens[W2_VALUE_INDEX];
            String countOverall = valueTokens[COUNT_OVERALL_VALUE_INDEX];
            String bigramCountInDecade = valueTokens[BIGRAM_COUNT_IN_DECADE_INDEX];
            String valueOut = String.format("%s,%s,%s,%s,%s,%s",
                    w1, w2, countOverall, bigramCountInDecade,
                    keyTokens[W1_KEY_INDEX].equals("_") ? "_" : wCount,
                    keyTokens[W2_KEY_INDEX].equals("_") ? "_" : wCount);
            context.write(new Text(String.format("%s,%s,%s",
                    keyTokens[DECADE_KEY_INDEX], w1, w2)), new Text(valueOut));
        }
    }
}
