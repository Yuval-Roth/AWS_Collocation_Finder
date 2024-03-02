import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.IOException;

public class CollocationFinder {

    public static void main(String[] args) {
        try {
            Job job = Job.getInstance();
            job.setJarByClass(CollocationFinder.class);
            
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
