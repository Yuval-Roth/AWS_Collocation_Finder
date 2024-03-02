import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
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
public class Step1 extends Mapper<Text,Text,IntWritable,Text> {

    private static final int TOKENS_PER_LINE = 5;
    private static final int W1_INDEX = 0;
    private static final int W2_INDEX = 1;
    private static final int DECADE_INDEX = 2;
    private static final int COUNT_OVERALL_INDEX = 3;
    private static final int DISTINCT_BOOKS_COUNT_INDEX = 4;
    private String[] tokens;
    private Set<String> stopWords;
    public static final String BUCKET_NAME = "distributed-systems-2024-bucket-yuval-adi";
    private AmazonS3 s3;

    @Override
    protected void setup(Mapper<Text, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
        String stopWordsUrl = context.getConfiguration().get("stopWordsURL");
        s3 = AmazonS3Client.builder().withRegion(Regions.US_WEST_2).build();
        String stopWordsStr = downloadSmallFileFromS3(stopWordsUrl);
        stopWords = new HashSet<>(){{
            addAll(Set.of(stopWordsStr.split("\n")));
        }};
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



    private String downloadSmallFileFromS3(String key) {

        var r = s3.getObject(new GetObjectRequest(BUCKET_NAME, "files/"+key));

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
