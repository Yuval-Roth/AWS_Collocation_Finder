import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * emits the following format: decade -> w1,w2,count_overall
 */
public class Job1 extends Mapper<Text,Text,IntWritable,Text> {

    private static final int TOKENS_PER_LINE = 5;
    private static final int W1_INDEX = 0;
    private static final int W2_INDEX = 1;
    private static final int DECADE_INDEX = 2;
    private static final int COUNT_OVERALL_INDEX = 3;
    private static final int DISTINCT_BOOKS_COUNT_INDEX = 4;
    private String[] tokens;
    private Set<String> stopWords;

    public Job1() {
        //TODO: LOAD STOP WORDS
        stopWords = new HashSet<>();
    }

    @Override
    protected void map(Text key, Text value, Mapper<Text, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        tokens = new String[TOKENS_PER_LINE];
        while (tokenizer.hasMoreTokens()){
            for (int i = 0; i < TOKENS_PER_LINE; i++){
                if (tokenizer.hasMoreTokens()){
                    tokens[i] = tokenizer.nextToken();
                } else {
                    throw new RuntimeException("Expected " + TOKENS_PER_LINE + " tokens per line, but found " + (i-1));
                }
            }
            if(! bigramHasStopWords()){
                context.write(makeKey(), makeValue());
            }
        }
    }

    private boolean bigramHasStopWords() {
        return stopWords.contains(tokens[W1_INDEX]) || stopWords.contains(tokens[W2_INDEX]);
    }

    private Text makeValue() {
        return new Text ("%s,%s,%s".formatted(tokens[W1_INDEX], tokens[W2_INDEX], tokens[COUNT_OVERALL_INDEX]));
    }

    private IntWritable makeKey() {
        int decade = Integer.parseInt(tokens[DECADE_INDEX]);
        decade = (decade / 10) * 10; // round down to nearest decade
        return new IntWritable(decade);
    }
}
