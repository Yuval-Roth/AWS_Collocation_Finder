package utils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DescendingComparator extends WritableComparator {

    DescendingComparator() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return -super.compare(a, b);
    }
}
