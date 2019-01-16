package UE3.Task2;

import UE3.TemperaturePair;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortByTempComparator extends WritableComparator {


    protected SortByTempComparator() {
        super(TemperaturePair.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        DoubleWritable p1 = (DoubleWritable) a;
        DoubleWritable p2 = (DoubleWritable) b;

        int cmp = p1.compareTo(p2);
        if (cmp != 0) {
            return cmp;
        }
        return -p1.compareTo(p2);
    }
}
