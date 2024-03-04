import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class DecadesPartitioner extends Partitioner<Text, Text> {
    public int getPartition(Text key, Text value, int numPartitions) {
        String[] keyTokens = key.toString().split(",");
        return Integer.parseInt(String.valueOf(keyTokens[0].charAt(2))) % numPartitions;
    }
}
