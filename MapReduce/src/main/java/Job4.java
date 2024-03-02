import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Job4 extends Reducer<Text, Text, Text, Text> {

    private static final int DECADE_KEY_INDEX = 0;
    private static final int W1_KEY_INDEX = 1;
    private static final int W2_KEY_INDEX = 2;
    private static final int W1_VALUE_INDEX = 0;
    private static final int W2_VALUE_INDEX = 1;
    private static final int COUNT_OVERALL_VALUE_INDEX = 2;
    private static final int BIGRAM_COUNT_IN_DECADE_INDEX = 3;
    private static final int W1_COUNT_IN_DECADE_INDEX = 4;
    private static final int W2_COUNT_IN_DECADE_INDEX = 5;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] keyTokens = key.toString().split(",");
        int w1Count = 0;
        int w2Count = 0;
        for (Text value : values) {
            String[] valueTokens = value.toString().split(",");
            w1Count += valueTokens[W1_COUNT_IN_DECADE_INDEX].equals("_") ? 0 : Integer.parseInt(valueTokens[W1_COUNT_IN_DECADE_INDEX]);
            w2Count += valueTokens[W2_COUNT_IN_DECADE_INDEX].equals("_") ? 0 : Integer.parseInt(valueTokens[W2_COUNT_IN_DECADE_INDEX]);
        }
        for(Text value : values) {
            String[] valueTokens = value.toString().split(",");
            String w1 = valueTokens[W1_VALUE_INDEX];
            String w2 = valueTokens[W2_VALUE_INDEX];
            String countOverall = valueTokens[COUNT_OVERALL_VALUE_INDEX];
            String bigramCountInDecade = valueTokens[BIGRAM_COUNT_IN_DECADE_INDEX];
            double npmi = calculateNPMI(countOverall, bigramCountInDecade, w1Count, w2Count);
            context.write(new Text(String.format("%s,%s,%s",
                    keyTokens[DECADE_KEY_INDEX], w1, w2)), new Text(String.valueOf(npmi)));
        }
    }

    private double calculateNPMI(String countOverall, String bigramCountInDecade, int w1Count, int w2Count) {
        double c_w1_w2 = Double.parseDouble(countOverall);
        double c_w1 = w1Count;
        double c_w2 = w2Count;
        double N = Double.parseDouble(bigramCountInDecade);
        double pmi = Math.log(c_w1_w2) + Math.log(N) - Math.log(c_w1) - Math.log(c_w2);
        return -1 * pmi / Math.log(c_w1_w2 / N);
    }
}
