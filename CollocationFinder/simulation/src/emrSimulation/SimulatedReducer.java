package emrSimulation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.security.Credentials;

import java.io.IOException;
import java.net.URI;
import java.util.*;

public abstract class SimulatedReducer<KI,VI,KO,VO> extends Reducer<KI, VI, KO, VO> {

    private final Map<KI, Iterable<VI>> input;
    private final Context context;
    private final List<String> output;
    private final Configuration conf;

    public String getOutput(Comparator<String> comparator) {
        output.sort(comparator);
        return getOutput();
    }

    public String getOutput() {
        StringBuilder output = new StringBuilder();
        for (String text : this.output) {
            output.append(text).append("\n");
        }
        return output.toString();
    }

    public void run() throws IOException, InterruptedException {
        setup(context);
        while (context.nextKey()) {
            reduce(context.getCurrentKey(), context.getValues(), context);
        }
    }

    @Override
    protected abstract void setup(Context context) throws IOException, InterruptedException;

    @Override
    protected abstract void reduce(KI key, Iterable<VI> values, Context context) throws IOException, InterruptedException;

    public SimulatedReducer(Map<KI, Iterable<VI>> _input, Configuration _conf){
        conf = _conf;
        input = _input;
        output = new LinkedList<>();
        context = new Context(){

            private Map.Entry<KI, Iterable<VI>> current;
            private Iterator<Map.Entry<KI, Iterable<VI>>> iter;

            @Override
            public boolean nextKey() throws IOException, InterruptedException {
                if (iter == null) {
                    iter = input.entrySet().iterator();
                }
                if (iter.hasNext()) {
                    current = iter.next();
                    return true;
                }
                return false;
            }

            @Override
            public KI getCurrentKey() throws IOException, InterruptedException {
                return current.getKey();
            }

            @Override
            public Iterable<VI> getValues() throws IOException, InterruptedException {
                return current.getValue();
            }

            @Override
            public void write(KO key, VO value) throws IOException, InterruptedException {
                output.add("%s\t%s".formatted(key,value));
            }

            @Override
            public Configuration getConfiguration() {
                return conf;
            }

            @Override
            public void progress() {

            }

            @Override
            public Credentials getCredentials() {
                return null;
            }

            @Override
            public JobID getJobID() {
                return null;
            }

            @Override
            public int getNumReduceTasks() {
                return 0;
            }

            @Override
            public Path getWorkingDirectory() throws IOException {
                return null;
            }

            @Override
            public Class<?> getOutputKeyClass() {
                return null;
            }

            @Override
            public Class<?> getOutputValueClass() {
                return null;
            }

            @Override
            public Class<?> getMapOutputKeyClass() {
                return null;
            }

            @Override
            public Class<?> getMapOutputValueClass() {
                return null;
            }

            @Override
            public String getJobName() {
                return null;
            }

            @Override
            public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
                return null;
            }

            @Override
            public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() throws ClassNotFoundException {
                return null;
            }

            @Override
            public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() throws ClassNotFoundException {
                return null;
            }

            @Override
            public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() throws ClassNotFoundException {
                return null;
            }

            @Override
            public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() throws ClassNotFoundException {
                return null;
            }

            @Override
            public Class<? extends Partitioner<?, ?>> getPartitionerClass() throws ClassNotFoundException {
                return null;
            }

            @Override
            public RawComparator<?> getSortComparator() {
                return null;
            }

            @Override
            public String getJar() {
                return null;
            }

            @Override
            public RawComparator<?> getCombinerKeyGroupingComparator() {
                return null;
            }

            @Override
            public RawComparator<?> getGroupingComparator() {
                return null;
            }

            @Override
            public boolean getJobSetupCleanupNeeded() {
                return false;
            }

            @Override
            public boolean getTaskCleanupNeeded() {
                return false;
            }

            @Override
            public boolean getProfileEnabled() {
                return false;
            }

            @Override
            public String getProfileParams() {
                return null;
            }

            @Override
            public Configuration.IntegerRanges getProfileTaskRange(boolean isMap) {
                return null;
            }

            @Override
            public String getUser() {
                return null;
            }

            @Override
            public boolean getSymlink() {
                return false;
            }

            @Override
            public Path[] getArchiveClassPaths() {
                return new Path[0];
            }

            @Override
            public URI[] getCacheArchives() throws IOException {
                return new URI[0];
            }

            @Override
            public URI[] getCacheFiles() throws IOException {
                return new URI[0];
            }

            @Override
            public Path[] getLocalCacheArchives() throws IOException {
                return new Path[0];
            }

            @Override
            public Path[] getLocalCacheFiles() throws IOException {
                return new Path[0];
            }

            @Override
            public Path[] getFileClassPaths() {
                return new Path[0];
            }

            @Override
            public String[] getArchiveTimestamps() {
                return new String[0];
            }

            @Override
            public String[] getFileTimestamps() {
                return new String[0];
            }

            @Override
            public int getMaxMapAttempts() {
                return 0;
            }

            @Override
            public int getMaxReduceAttempts() {
                return 0;
            }

            @Override
            public TaskAttemptID getTaskAttemptID() {
                return null;
            }

            @Override
            public void setStatus(String msg) {

            }

            @Override
            public String getStatus() {
                return null;
            }

            @Override
            public float getProgress() {
                return 0;
            }

            @Override
            public Counter getCounter(Enum<?> counterName) {
                return null;
            }

            @Override
            public Counter getCounter(String groupName, String counterName) {
                return null;
            }

            @Override
            public VI getCurrentValue() throws IOException, InterruptedException {
                return null;
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                return false;
            }

            @Override
            public OutputCommitter getOutputCommitter() {
                return null;
            }
        };
    }
}
