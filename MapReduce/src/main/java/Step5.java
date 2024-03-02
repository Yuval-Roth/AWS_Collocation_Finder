import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Step5 extends Reducer<Text, Text, Text, Text> {

    private static final int W1_VALUE_INDEX = 0;
    private static final int W2_VALUE_INDEX = 1;
    private static final int NPMI_VALUE_INDEX = 2;
    private double relMinPmi;


    @Override
    protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        relMinPmi = Double.parseDouble(context.getConfiguration().get("relMinPmi"));
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String decade = key.toString();
        double npmiTotal = 0;
        for(Text value : values){
            String[] valueTokens = value.toString().split(",");
            npmiTotal += Double.parseDouble(valueTokens[NPMI_VALUE_INDEX]);
        }
        for (Text value : values) {
            String[] valueTokens = value.toString().split(",");
            String w1 = valueTokens[W1_VALUE_INDEX];
            String w2 = valueTokens[W2_VALUE_INDEX];
            double npmi = Double.parseDouble(valueTokens[NPMI_VALUE_INDEX]);
            double relNpmi = npmi / npmiTotal;
            if (relNpmi < relMinPmi) {
                continue;
            }
            context.write(new Text("%s,%f,%s,%s".formatted(decade,npmi,w1,w2)), new Text(""));
        }
    }

}
